"""
Daily Follow-Up Agent for Harper Insurance.
Identifies accounts needing follow-up and plans/executes actions.
"""
import asyncio
import os
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum
import asyncpg
from anthropic import Anthropic

from query_v2 import MemoryQueryEngine


class FollowUpChannel(Enum):
    EMAIL = "email"
    CALL = "call"
    SMS = "sms"


class FollowUpPriority(Enum):
    CRITICAL = 1  # Same day
    HIGH = 2      # Within 24 hours
    MEDIUM = 3    # Within 48 hours
    LOW = 4       # Within week


@dataclass
class FollowUpAction:
    account_id: int
    account_name: str
    stage: str
    stage_detail: Optional[str]
    priority: FollowUpPriority
    channel: FollowUpChannel
    reason: str
    context_summary: str
    specific_issues: Optional[List[str]] = None
    draft_content: Optional[str] = None
    suggested_time: Optional[datetime] = None
    assigned_to: Optional[str] = None


@dataclass
class DailyPlan:
    generated_at: datetime
    total_actions: int
    by_priority: Dict[str, int]
    by_channel: Dict[str, int]
    by_stage: Dict[str, int]
    actions: List[FollowUpAction]


class FollowUpAgent:
    """Agent that identifies and plans follow-up actions across the brokerage."""
    
    # Stage-specific follow-up rules
    STAGE_RULES = {
        "intake": {
            "stale_days": 2,
            "typical_needs": ["Missing information", "Document collection", "Initial qualification"],
            "preferred_channels": [FollowUpChannel.EMAIL, FollowUpChannel.CALL],
        },
        "application": {
            "stale_days": 3,
            "typical_needs": ["Form completion", "Correction requests", "Data gathering"],
            "preferred_channels": [FollowUpChannel.EMAIL, FollowUpChannel.SMS],
        },
        "submission": {
            "stale_days": 2,
            "typical_needs": ["Underwriter communications", "Information requests", "Quote tracking"],
            "preferred_channels": [FollowUpChannel.EMAIL, FollowUpChannel.CALL],
        },
        "quote": {
            "stale_days": 1,
            "typical_needs": ["Pricing discussions", "Coverage questions", "Competitor responses"],
            "preferred_channels": [FollowUpChannel.CALL, FollowUpChannel.EMAIL],
        },
        "closing": {
            "stale_days": 1,
            "typical_needs": ["Payment follow-up", "Document signing", "Bind confirmation"],
            "preferred_channels": [FollowUpChannel.CALL, FollowUpChannel.SMS],
        },
        "lead": {
            "stale_days": 3,
            "typical_needs": ["Initial outreach", "Qualification", "Interest confirmation"],
            "preferred_channels": [FollowUpChannel.EMAIL, FollowUpChannel.CALL],
        },
    }
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
        self.client = Anthropic()
        self.query_engine = MemoryQueryEngine(db_pool)
    
    async def generate_daily_plan(self) -> DailyPlan:
        """Generate a daily follow-up plan for all accounts needing attention."""
        print("🔍 Scanning accounts for follow-up needs...")
        
        # Get all accounts with their event history
        accounts_needing_followup = await self._identify_accounts_needing_followup()
        
        print(f"📋 Found {len(accounts_needing_followup)} accounts needing follow-up")
        
        # Generate actions for each account
        actions = []
        for account in accounts_needing_followup:
            print(f"  Processing: {account['name']}...")
            action = await self._plan_followup_action(account)
            if action:
                actions.append(action)
        
        # Sort by priority
        actions.sort(key=lambda x: x.priority.value)
        
        # Build plan summary
        plan = DailyPlan(
            generated_at=datetime.now(timezone.utc),
            total_actions=len(actions),
            by_priority={p.name: sum(1 for a in actions if a.priority == p) for p in FollowUpPriority},
            by_channel={c.name: sum(1 for a in actions if a.channel == c) for c in FollowUpChannel},
            by_stage={},
            actions=actions
        )
        
        # Count by stage
        for action in actions:
            stage = action.stage
            plan.by_stage[stage] = plan.by_stage.get(stage, 0) + 1
        
        return plan
    
    async def _identify_accounts_needing_followup(self) -> List[Dict]:
        """Query database for accounts that need follow-up."""
        async with self.pool.acquire() as conn:
            # Get accounts with their last event and stage info
            accounts = await conn.fetch("""
                WITH account_events AS (
                    SELECT 
                        a.id,
                        a.name,
                        a.stage,
                        a.stage_detail,
                        a.industry,
                        a.primary_email,
                        a.primary_phone,
                        COUNT(e.id) as event_count,
                        MAX(e.occurred_at) as last_event_at,
                        MAX(CASE WHEN e.direction IN ('outbound', 'outgoing') THEN e.occurred_at END) as last_outbound_at,
                        MAX(CASE WHEN e.direction IN ('inbound', 'incoming') THEN e.occurred_at END) as last_inbound_at,
                        array_agg(DISTINCT e.event_type) FILTER (WHERE e.occurred_at > NOW() - INTERVAL '7 days') as recent_event_types
                    FROM accounts a
                    LEFT JOIN events e ON a.id = e.account_id
                    WHERE a.stage NOT IN ('closed', 'lost', 'policy')  -- Active accounts only
                    GROUP BY a.id
                )
                SELECT *,
                    EXTRACT(EPOCH FROM (NOW() - last_event_at)) / 86400 as days_since_last_event,
                    EXTRACT(EPOCH FROM (NOW() - last_outbound_at)) / 86400 as days_since_outbound,
                    EXTRACT(EPOCH FROM (NOW() - last_inbound_at)) / 86400 as days_since_inbound
                FROM account_events
                WHERE 
                    -- No events in last N days (stage-dependent)
                    last_event_at < NOW() - INTERVAL '1 day'
                    OR last_event_at IS NULL
                    -- Or we received inbound but haven't responded
                    OR (last_inbound_at > last_outbound_at AND last_inbound_at < NOW() - INTERVAL '4 hours')
                ORDER BY 
                    CASE stage
                        WHEN 'closing' THEN 1
                        WHEN 'quote' THEN 2
                        WHEN 'submission' THEN 3
                        WHEN 'application' THEN 4
                        WHEN 'intake' THEN 5
                        WHEN 'lead' THEN 6
                        ELSE 7
                    END,
                    last_event_at ASC NULLS FIRST
                LIMIT 5
            """)
            
            return [dict(a) for a in accounts]
    
    async def _plan_followup_action(self, account: Dict) -> Optional[FollowUpAction]:
        """Plan a specific follow-up action for an account using LLM."""
        stage = account['stage'].lower()
        rules = self.STAGE_RULES.get(stage, self.STAGE_RULES['lead'])
        
        # Get recent context from query engine
        context = await self._get_account_context(account['id'], account['name'])
        
        # Determine priority
        days_stale = account.get('days_since_last_event') or 999
        awaiting_response = (account.get('last_inbound_at') and 
                           account.get('last_outbound_at') and
                           account['last_inbound_at'] > account['last_outbound_at'])
        
        if awaiting_response:
            priority = FollowUpPriority.CRITICAL
        elif days_stale > rules['stale_days'] * 2:
            priority = FollowUpPriority.HIGH
        elif days_stale > rules['stale_days']:
            priority = FollowUpPriority.MEDIUM
        else:
            priority = FollowUpPriority.LOW
        
        # Use LLM to determine best action and draft content
        action_plan = await self._get_llm_action_plan(account, context, rules, priority)
        
        if not action_plan:
            return None
        
        return FollowUpAction(
            account_id=account['id'],
            account_name=account['name'],
            stage=account['stage'],
            stage_detail=account.get('stage_detail'),
            priority=priority,
            channel=action_plan['channel'],
            reason=action_plan['reason'],
            context_summary=action_plan['context_summary'],
            specific_issues=action_plan.get('specific_issues_to_address', []),
            draft_content=action_plan.get('draft_content'),
            suggested_time=action_plan.get('suggested_time'),
        )
    
    async def _get_account_context(self, account_id: int, account_name: str) -> Dict[str, str]:
        """Get detailed context for an account using query engine."""
        try:
            context = {}
            
            # 1. Get recent communications summary
            results = await self.query_engine.search.search_account_events(
                query="recent communications status updates",
                account_id=account_id,
                top_k=5,
                min_similarity=0.1
            )
            
            if not results:
                results = await self.query_engine._get_recent_events(account_id, limit=5)
            
            if results:
                lines = []
                for r in results:
                    date_str = r.occurred_at.strftime('%Y-%m-%d')
                    content = r.summary or (r.raw_content[:200] if r.raw_content else "No content")
                    lines.append(f"[{date_str}] {r.event_type}: {content}")
                context["recent_communications"] = "\n".join(lines)
            else:
                context["recent_communications"] = "No recent communications found."
            
            # 2. Query for specific concerns/objections
            concerns_results = await self.query_engine.search.search_account_events(
                query="concerns objections problems issues worried price cost",
                account_id=account_id,
                top_k=3,
                min_similarity=0.3
            )
            if concerns_results:
                concerns = [r.summary or r.raw_content[:150] for r in concerns_results]
                context["concerns"] = "\n".join(f"- {c}" for c in concerns)
            else:
                context["concerns"] = "No specific concerns identified."
            
            # 3. Query for pending items/requests
            pending_results = await self.query_engine.search.search_account_events(
                query="waiting for need requested documents missing pending",
                account_id=account_id,
                top_k=3,
                min_similarity=0.3
            )
            if pending_results:
                pending = [r.summary or r.raw_content[:150] for r in pending_results]
                context["pending_items"] = "\n".join(f"- {p}" for p in pending)
            else:
                context["pending_items"] = "No pending items identified."
            
            # 4. Query for last substantive interaction
            last_interaction = await self.query_engine.search.search_account_events(
                query="discussed agreed next steps decision",
                account_id=account_id,
                top_k=1,
                min_similarity=0.2
            )
            if last_interaction:
                r = last_interaction[0]
                context["last_interaction"] = f"[{r.occurred_at.strftime('%Y-%m-%d')}] {r.summary or r.raw_content[:200]}"
            else:
                context["last_interaction"] = "No substantive interaction found."
            
            return context
            
        except Exception as e:
            return {
                "recent_communications": f"Error retrieving context: {e}",
                "concerns": "Unknown",
                "pending_items": "Unknown",
                "last_interaction": "Unknown"
            }
    
    async def _get_llm_action_plan(
        self, 
        account: Dict, 
        context: str, 
        rules: Dict,
        priority: FollowUpPriority
    ) -> Optional[Dict]:
        """Use LLM to determine the best follow-up action."""
        
        prompt = f"""You are a follow-up planning assistant for Harper Insurance brokerage.

ACCOUNT INFORMATION:
- Name: {account['name']}
- Stage: {account['stage']} {('/ ' + account['stage_detail']) if account.get('stage_detail') else ''}
- Industry: {account.get('industry') or 'Unknown'}
- Days since last contact: {account.get('days_since_last_event', 'Unknown')}
- Has email: {'Yes' if account.get('primary_email') else 'No'}
- Has phone: {'Yes' if account.get('primary_phone') else 'No'}

RECENT COMMUNICATIONS:
{context.get('recent_communications', 'None available')}

IDENTIFIED CONCERNS/OBJECTIONS:
{context.get('concerns', 'None identified')}

PENDING ITEMS/REQUESTS:
{context.get('pending_items', 'None identified')}

LAST SUBSTANTIVE INTERACTION:
{context.get('last_interaction', 'None found')}

STAGE-SPECIFIC GUIDANCE:
- Typical needs at this stage: {', '.join(rules['typical_needs'])}
- Preferred channels: {', '.join(c.value for c in rules['preferred_channels'])}

TASK:
Based on ALL the context above:
1. Identify the SPECIFIC issue or topic that needs to be addressed
2. Determine the most appropriate follow-up action and channel
3. Draft personalized follow-up content that directly addresses the identified issues

Respond in this exact JSON format:
{{
    "channel": "email" or "call" or "sms",
    "reason": "Specific reason for follow-up based on the context",
    "context_summary": "1-2 sentence summary of the situation and what specifically needs to be addressed",
    "specific_issues_to_address": ["Issue 1 from context", "Issue 2 from context"],
    "draft_content": "The actual draft email/SMS text or call talking points - be specific and reference actual details from the context",
    "call_script": "If channel is call, include specific talking points addressing the identified issues"
}}"""""

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse JSON from response
            text = response.content[0].text
            
            # Extract JSON
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]
            
            result = json.loads(text.strip())
            
            # Map channel string to enum
            channel_map = {"email": FollowUpChannel.EMAIL, "call": FollowUpChannel.CALL, "sms": FollowUpChannel.SMS}
            result['channel'] = channel_map.get(result.get('channel', 'email').lower(), FollowUpChannel.EMAIL)
            
            return result
            
        except Exception as e:
            print(f"    Error getting LLM plan: {e}")
            # Fallback to simple rule-based action
            return {
                "channel": rules['preferred_channels'][0],
                "reason": f"Account has been inactive for {account.get('days_since_last_event', 'several')} days",
                "context_summary": "Unable to analyze context - defaulting to standard follow-up",
                "draft_content": None
            }
    
    async def execute_action(self, action: FollowUpAction, dry_run: bool = True) -> Dict:
        """Execute a follow-up action (send email, log call, send SMS)."""
        result = {
            "action_id": f"{action.account_id}_{datetime.now().timestamp()}",
            "account_id": action.account_id,
            "account_name": action.account_name,
            "channel": action.channel.value,
            "status": "pending",
            "dry_run": dry_run
        }
        
        if dry_run:
            result["status"] = "simulated"
            result["message"] = f"[DRY RUN] Would send {action.channel.value} to {action.account_name}"
            print(f"  📧 [DRY RUN] {action.channel.value.upper()} to {action.account_name}")
            return result
        
        # Actual execution would go here
        if action.channel == FollowUpChannel.EMAIL:
            result = await self._send_email(action)
        elif action.channel == FollowUpChannel.SMS:
            result = await self._send_sms(action)
        elif action.channel == FollowUpChannel.CALL:
            result = await self._schedule_call(action)
        
        # Log the action as an event
        await self._log_action_as_event(action, result)
        
        return result
    
    async def _send_email(self, action: FollowUpAction) -> Dict:
        """Send follow-up email. (Mock implementation)"""
        # In production, integrate with email service (SendGrid, SES, etc.)
        print(f"  📧 Sending email to {action.account_name}...")
        return {"status": "sent", "channel": "email", "message": "Email sent successfully"}
    
    async def _send_sms(self, action: FollowUpAction) -> Dict:
        """Send follow-up SMS. (Mock implementation)"""
        # In production, integrate with SMS service (Twilio, etc.)
        print(f"  💬 Sending SMS to {action.account_name}...")
        return {"status": "sent", "channel": "sms", "message": "SMS sent successfully"}
    
    async def _schedule_call(self, action: FollowUpAction) -> Dict:
        """Schedule follow-up call. (Mock implementation)"""
        # In production, integrate with calendar/dialer
        print(f"  📞 Scheduling call with {action.account_name}...")
        return {"status": "scheduled", "channel": "call", "message": "Call scheduled"}
    
    async def _log_action_as_event(self, action: FollowUpAction, result: Dict):
        """Log the follow-up action as an event in the database."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO events (
                    account_id, event_type, channel, direction, 
                    occurred_at, raw_content, summary, extraction_status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
                action.account_id,
                f"followup_{action.channel.value}",
                action.channel.value,
                "outbound",
                datetime.now(timezone.utc),
                action.draft_content or f"Follow-up {action.channel.value}: {action.reason}",
                f"Automated follow-up: {action.reason}",
                "complete"
            )


def format_daily_plan(plan: DailyPlan) -> str:
    """Format the daily plan for display."""
    lines = [
        "=" * 60,
        f"📅 DAILY FOLLOW-UP PLAN",
        f"Generated: {plan.generated_at.strftime('%Y-%m-%d %H:%M %Z')}",
        "=" * 60,
        "",
        f"📊 SUMMARY",
        f"Total actions needed: {plan.total_actions}",
        "",
        "By Priority:",
    ]
    
    priority_emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}
    for priority, count in plan.by_priority.items():
        if count > 0:
            lines.append(f"  {priority_emoji.get(priority, '⚪')} {priority}: {count}")
    
    lines.append("")
    lines.append("By Channel:")
    channel_emoji = {"EMAIL": "📧", "CALL": "📞", "SMS": "💬"}
    for channel, count in plan.by_channel.items():
        if count > 0:
            lines.append(f"  {channel_emoji.get(channel, '📌')} {channel}: {count}")
    
    lines.append("")
    lines.append("By Stage:")
    for stage, count in plan.by_stage.items():
        lines.append(f"  • {stage}: {count}")
    
    lines.append("")
    lines.append("=" * 60)
    lines.append("📋 ACTION ITEMS")
    lines.append("=" * 60)
    
    for i, action in enumerate(plan.actions, 1):
        priority_emoji_map = {
            FollowUpPriority.CRITICAL: "🔴",
            FollowUpPriority.HIGH: "🟠",
            FollowUpPriority.MEDIUM: "🟡",
            FollowUpPriority.LOW: "🟢"
        }
        channel_emoji_map = {
            FollowUpChannel.EMAIL: "📧",
            FollowUpChannel.CALL: "📞",
            FollowUpChannel.SMS: "💬"
        }
        
        lines.append(f"\n{i}. {priority_emoji_map[action.priority]} [{action.priority.name}] {action.account_name}")
        lines.append(f"   Stage: {action.stage}" + (f" / {action.stage_detail}" if action.stage_detail else ""))
        lines.append(f"   Channel: {channel_emoji_map[action.channel]} {action.channel.value.upper()}")
        lines.append(f"   Reason: {action.reason}")
        lines.append(f"   Context: {action.context_summary}")
        
        if action.specific_issues:
            lines.append(f"   🎯 Specific Issues to Address:")
            for issue in action.specific_issues:
                lines.append(f"      • {issue}")
        
        if action.draft_content:
            lines.append(f"   Draft:")
            for draft_line in action.draft_content.split('\n')[:5]:  # First 5 lines
                lines.append(f"      {draft_line}")
            if len(action.draft_content.split('\n')) > 5:
                lines.append(f"      ...")
    
    return "\n".join(lines)


async def main():
    """Run the daily follow-up agent."""
    import sys
    
    pool = await asyncpg.create_pool(
        os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")
    )
    
    agent = FollowUpAgent(pool)
    
    print("🤖 Harper Insurance Follow-Up Agent")
    print("=" * 40)
    
    if len(sys.argv) > 1 and sys.argv[1] == "execute":
        # Execute mode - actually perform actions
        dry_run = "--dry-run" in sys.argv
        
        print(f"Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
        print("")
        
        plan = await agent.generate_daily_plan()
        print(format_daily_plan(plan))
        
        if plan.actions:
            print("\n" + "=" * 60)
            print("🚀 EXECUTING ACTIONS...")
            print("=" * 60)
            
            for action in plan.actions:
                result = await agent.execute_action(action, dry_run=dry_run)
                print(f"  {result['status']}: {result.get('message', '')}")
    else:
        # Plan mode - just generate and display plan
        print("Mode: PLANNING ONLY (use 'execute' argument to take action)")
        print("")
        
        plan = await agent.generate_daily_plan()
        print(format_daily_plan(plan))
        
        # Save plan to file
        plan_file = f"followup_plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(plan_file, 'w') as f:
            # Convert to serializable format
            plan_dict = {
                "generated_at": plan.generated_at.isoformat(),
                "total_actions": plan.total_actions,
                "by_priority": plan.by_priority,
                "by_channel": plan.by_channel,
                "by_stage": plan.by_stage,
                "actions": [
                    {
                        **asdict(a),
                        "priority": a.priority.name,
                        "channel": a.channel.value,
                        "suggested_time": a.suggested_time.isoformat() if a.suggested_time else None
                    }
                    for a in plan.actions
                ]
            }
            json.dump(plan_dict, f, indent=2)
        print(f"\n📁 Plan saved to: {plan_file}")
    
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())