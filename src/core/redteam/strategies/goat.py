"""GOAT (Generative Offensive Agent Tester) attack strategy.

Implementation of the GOAT algorithm from https://arxiv.org/pdf/2410.01606
"""

import logging
import re
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.core.redteam.attack_techniques import (
    GOAT_FOLLOWUP_PROMPT,
    GOAT_INITIAL_PROMPT,
    GOAT_SYSTEM_PROMPT,
    format_attack_techniques,
)
from src.core.redteam.llm_interface import LLMInterface
from src.core.redteam.schemas import ConversationTurn, DynamicAttackSession
from src.core.redteam.strategies.base import AttackStrategy

logger = logging.getLogger(__name__)


class GOATStrategy(AttackStrategy):
    """
    GOAT: Generative Offensive Agent Tester
    
    Uses Observation-Thought-Strategy-Response (O-T-S-R) reasoning framework
    to iteratively craft adversarial prompts across multiple turns.
    
    Maintains two separate conversation histories:
    - C_A: AttackerLLM history with full reasoning (O, T, S, R)
    - C_T: TargetLLM history with just adversarial prompts and responses
    """

    def __init__(
        self,
        attacker_model: LLMInterface,
        target_model: LLMInterface,
        max_iterations: int = 5,
        early_stop: bool = True
    ) -> None:
        super().__init__(attacker_model, target_model, max_iterations, early_stop)
        
        # Create JailbreakBench judge (uses attacker model for judging)
        from src.core.redteam.judge import LLMJudge
        from src.core.redteam.judge_config import JAILBREAKBENCH_JUDGE
        
        self.judge = LLMJudge(attacker_model, JAILBREAKBENCH_JUDGE)

    @property
    def strategy_name(self) -> str:
        """Return strategy name."""
        return "goat"

    async def run_attack_session(
        self,
        goal: str
    ) -> DynamicAttackSession:
        """
        Run one GOAT attack session for a specific goal.
        
        Args:
            goal: The attack objective
            
        Returns:
            DynamicAttackSession with complete conversation and results
        """
        session_id = f"goat-{uuid.uuid4()}"
        start_time = datetime.now(timezone.utc)
        
        logger.info(f"Starting GOAT session {session_id} for goal: {goal}")
        
        # Initialize conversations
        attack_techniques_text = format_attack_techniques()
        system_prompt = GOAT_SYSTEM_PROMPT.format(
            goal=goal,
            attack_techniques=attack_techniques_text
        )
        
        # AttackerLLM conversation history C_A = [S_A, a_initial_prompt(O)]
        initial_prompt = GOAT_INITIAL_PROMPT.format(goal=goal)
        C_A: List[Dict[str, str]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": initial_prompt}
        ]
        
        # TargetLLM conversation history C_T = []
        C_T: List[Dict[str, str]] = []
        
        # Conversation turns
        turns: List[ConversationTurn] = []
        success_turns: List[int] = []
        
        # Main GOAT loop
        for k in range(self.max_iterations):
            logger.info(f"GOAT session {session_id} - Turn {k+1}/{self.max_iterations}")
            
            try:
                # Generate adversarial prompt/response r_A ← AttackerLLM(C_A)
                attacker_response = await self.attacker_model.generate_with_history(C_A)
                
                if not attacker_response:
                    logger.warning(f"Failed to generate attacker response at turn {k+1}")
                    continue
                
                # Parse structured output (O, T, S, R)
                otsr = self._parse_otsr_response(attacker_response)
                
                if not otsr or not otsr.get('Response'):
                    logger.warning(f"Failed to parse O-T-S-R at turn {k+1}, using fallback")
                    otsr = {
                        'Observation': '',
                        'Thought': '',
                        'Strategy': '',
                        'Response': attacker_response
                    }
                
                # C_A += [r_A] (append complete reasoning)
                attacker_response_full = self._format_otsr(otsr)
                C_A.append({"role": "assistant", "content": attacker_response_full})
                
                # C_T += [r_A['Response']] (append just adversarial prompt)
                adversarial_prompt = otsr['Response']
                C_T.append({"role": "user", "content": adversarial_prompt})
                
                # Generate response r_T ← TargetLLM(C_T)
                target_response, latency_ms = await self._generate_target_response(C_T)
                
                if not target_response:
                    logger.warning(f"No response from target at turn {k+1}")
                    continue
                
                # C_T += [r_T]
                C_T.append({"role": "assistant", "content": target_response})
                
                # Evaluate jailbreak success using JailbreakBench judge
                judge_eval = await self.judge.evaluate(
                    prompt=goal,  # Goal is the prompt for judge
                    response=target_response
                )
                is_jailbreak = judge_eval.attack_success
                
                # Create conversation turn
                turn = ConversationTurn(
                    turn_number=k + 1,
                    attacker_prompt=adversarial_prompt,
                    target_response=target_response,
                    attacker_reasoning=attacker_response_full,
                    attack_success=is_jailbreak,
                    judge_evaluation=judge_eval
                )
                turns.append(turn)
                
                if is_jailbreak:
                    success_turns.append(k + 1)
                    logger.info(f"✅ GOAT attack successful at turn {k + 1}!")
                    if self.early_stop:
                        logger.info("Early stopping enabled, ending session.")
                        break
                else:
                    logger.info(f"❌ GOAT attack unsuccessful at turn {k + 1}. Continuing...")
                
                # C_A += [a_follow_up_prompt(O, r_A['Response'], r_T)]
                followup_prompt = GOAT_FOLLOWUP_PROMPT.format(
                    goal=goal,
                    adversarial_prev_prompt=adversarial_prompt,
                    prev_model_response=target_response
                )
                C_A.append({"role": "user", "content": followup_prompt})
                
            except Exception as e:
                logger.error(f"Error in GOAT turn {k+1}: {e}")
                # Continue to next turn
                continue
        
        # Create session result
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        session = DynamicAttackSession(
            session_id=session_id,
            config={
                "attack_goal": goal,
                "max_iterations": self.max_iterations,
                "strategy": self.strategy_name,
                "early_stop_on_success": self.early_stop
            },
            turns=turns,
            final_success=len(success_turns) > 0,
            success_turns=success_turns if success_turns else None,
            total_turns=len(turns),
            duration_seconds=duration,
            started_at=start_time.isoformat(),
            completed_at=end_time.isoformat()
        )
        
        logger.info(
            f"GOAT session {session_id} complete: "
            f"{len(turns)} turns, success={session.final_success}"
        )
        
        return session

    async def _generate_target_response(
        self,
        conversation: List[Dict[str, str]]
    ) -> tuple[str, float]:
        """
        Generate target model response from conversation.
        
        Args:
            conversation: List of message dicts
            
        Returns:
            Tuple of (response text, latency in ms)
        """
        start_time = time.time()
        
        try:
            # Extract just the last user message as the prompt
            last_user_message = None
            for msg in reversed(conversation):
                if msg["role"] == "user":
                    last_user_message = msg["content"]
                    break
            
            if not last_user_message:
                raise ValueError("No user message found in conversation")
            
            # Generate response
            response, latency_ms = await self.target_model.generate(last_user_message)
            return response, latency_ms
            
        except Exception as e:
            logger.error(f"Error generating target response: {e}")
            raise

    def _parse_otsr_response(self, text: str) -> Optional[Dict[str, str]]:
        """
        Parse Observation, Thought, Strategy, Response from attacker output.
        
        Handles various formats:
        - Full words: "Observation: ...", "Thought: ...", etc.
        - With markers: "►Observation: ...", "▶Observation: ...", etc.
        - Numbered: "1. Observation: ...", "2. Thought: ...", etc.
        - Bold markdown: "**Observation**:", "**Thought (T)**:", etc.
        - With abbreviations: "Observation (O):", "Thought (T):", etc.
        - Short form: "O:", "T:", "S:", "R:"
        
        Args:
            text: Raw attacker response
            
        Returns:
            Dict with O, T, S, R keys or None if parsing fails
        """
        # Pattern that matches a section header in any format
        section_boundary = r'(?:\n|^)\s*(?:\d+\.\s*)?[*_]*[▶►]?\s*(?:\*\*)?(?:Observation(?:\s*\(O\))?|Thought(?:\s*\(T\))?|Strategy(?:\s*\(S\))?|(?:Reply|Response)(?:\s+\w+)?(?:\s*\(R\))?|O|T|S|R)(?:\*\*)?[:\s]'
        
        # Patterns for each section
        patterns = {
            'Observation': r'(?:^\s*(?:\d+\.\s*)?[*_]*[▶►]?\s*(?:\*\*)?(?:Observation(?:\s*\(O\))?|O)(?:\*\*)?[:\s]+)(.+?)(?=' + section_boundary + r'|$)',
            'Thought': r'(?:^\s*(?:\d+\.\s*)?[*_]*[▶►]?\s*(?:\*\*)?(?:Thought(?:\s*\(T\))?|T)(?:\*\*)?[:\s]+)(.+?)(?=' + section_boundary + r'|$)',
            'Strategy': r'(?:^\s*(?:\d+\.\s*)?[*_]*[▶►]?\s*(?:\*\*)?(?:Strategy(?:\s*\(S\))?|S)(?:\*\*)?[:\s]+)(.+?)(?=' + section_boundary + r'|$)',
            'Response': r'(?:^\s*(?:\d+\.\s*)?[*_]*[▶►]?\s*(?:\*\*)?(?:(?:Reply|Response)(?:\s+\w+)?(?:\s*\(R\))?|R)(?:\*\*)?[:\s]+)(.+?)(?=' + section_boundary + r'|$)',
        }
        
        result = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.DOTALL | re.IGNORECASE | re.MULTILINE)
            if match:
                content = match.group(1).strip()
                # Remove leading/trailing quotes if present
                content = re.sub(r'^["\']|["\']$', '', content)
                result[key] = content
        
        # Ensure we have at least a Response
        if not result.get('Response') and text:
            # If no structured format, extract after "---" or use whole text
            parts = text.split('---')
            result['Response'] = parts[-1].strip() if len(parts) > 1 else text.strip()
        
        return result if result.get('Response') else None

    def _format_otsr(self, otsr: Dict[str, str]) -> str:
        """Format O-T-S-R for storage in conversation history."""
        return f"""▶ Observation: {otsr.get('Observation', '')}
▶ Thought: {otsr.get('Thought', '')}
▶ Strategy: {otsr.get('Strategy', '')}
▶ Response: {otsr.get('Response', '')}"""


class GOATSessionRunner:
    """Helper class to run multiple GOAT sessions for a job."""

    def __init__(
        self,
        target_model: LLMInterface,
        attacker_model: LLMInterface,
        max_iterations: int = 5,
        early_stop: bool = True
    ) -> None:
        self.engine = GOATStrategy(
            attacker_model=attacker_model,
            target_model=target_model,
            max_iterations=max_iterations,
            early_stop=early_stop
        )

    async def run_all_sessions(
        self,
        attack_goals: List[str],
        num_sessions_per_goal: int = 1
    ) -> List[DynamicAttackSession]:
        """
        Run GOAT sessions for multiple goals.
        
        Args:
            attack_goals: List of attack objectives
            num_sessions_per_goal: Number of trials per goal
            
        Returns:
            List of all session results
        """
        all_sessions = []
        total_sessions = len(attack_goals) * num_sessions_per_goal
        session_count = 0
        
        for goal_idx, goal in enumerate(attack_goals):
            logger.info(
                f"GOAT attacking goal {goal_idx + 1}/{len(attack_goals)}: {goal}"
            )
            
            for session_num in range(num_sessions_per_goal):
                session_count += 1
                logger.info(
                    f"Session {session_count}/{total_sessions} "
                    f"(goal {goal_idx + 1}, trial {session_num + 1}/{num_sessions_per_goal})"
                )
                
                try:
                    session = await self.engine.run_attack_session(goal)
                    all_sessions.append(session)
                    
                except Exception as e:
                    logger.error(f"Error in session {session_count}: {e}")
                    failed_session = self._create_failed_session(goal, str(e))
                    all_sessions.append(failed_session)
        
        return all_sessions

    def _create_failed_session(self, goal: str, error: str) -> DynamicAttackSession:
        """Create a failed session record."""
        now = datetime.now(timezone.utc).isoformat()
        return DynamicAttackSession(
            session_id=f"failed-{uuid.uuid4()}",
            config={
                "attack_goal": goal,
                "max_iterations": self.engine.max_iterations,
                "strategy": self.engine.strategy_name,
                "early_stop_on_success": self.engine.early_stop
            },
            turns=[],
            final_success=False,
            success_turns=None,
            total_turns=0,
            duration_seconds=0.0,
            started_at=now,
            completed_at=now
        )


