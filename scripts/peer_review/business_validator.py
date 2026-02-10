"""
╔══════════════════════════════════════════════════════════════════╗
║                   BUSINESS VALIDATOR                             ║
╚══════════════════════════════════════════════════════════════════╝

Predicts business failures and metric drift using LLM analysis.

USAGE:
    from peer_review.business_validator import BusinessValidator
    
    validator = BusinessValidator(api_key='your-gemini-api-key')
    report = validator.validate(
        old_code=old_sql,
        new_code=new_sql,
        context={'table': 'silver.orders', 'downstream': ['gold.revenue_dashboard']}
    )
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import logging
import json
import re

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import google.generativeai as genai
except ImportError:
    genai = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class BusinessImpact:
    """Represents predicted business impact."""
    severity: str  # 'HIGH', 'MEDIUM', 'LOW'
    predicted_shift: str  # Human-readable prediction
    metric_drift_detected: bool
    affected_metrics: List[str]
    confidence: float  # 0.0 to 1.0
    details: Dict[str, Any]
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class BusinessValidationReport:
    """Result of business validation."""
    business_impact: Dict[str, Any]
    risk_level: str  # 'HIGH', 'MEDIUM', 'LOW', 'NONE'
    summary: str
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROMPT TEMPLATES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BUSINESS_ANALYSIS_PROMPT = """
You are a Senior Data Architect analyzing code changes for business impact.

CONTEXT:
- Table: {table_name}
- Downstream Dependencies: {downstream_tables}
- Business Metrics Affected: {affected_metrics}

OLD CODE:
```sql
{old_code}
```

NEW CODE:
```sql
{new_code}
```

TASK:
Analyze the shift in calculation logic between OLD_CODE and NEW_CODE.
Focus on the MEANING of the data, not just technical correctness.

INSTRUCTIONS:
1. Predict how the change will shift key business metrics
   Example: "This change will likely reduce Daily Revenue by 2-5% due to the new exclusion criteria"

2. Identify "Metric Drift" - where the name remains the same but underlying definition changed
   Example: "Column 'active_users' changed from 'last 30 days' to 'last 7 days'"

3. Assess confidence level (0.0 to 1.0) in your predictions

OUTPUT FORMAT (JSON):
{{
  "severity": "HIGH|MEDIUM|LOW",
  "predicted_shift": "One-sentence prediction of how metrics will change",
  "metric_drift_detected": true/false,
  "affected_metrics": ["metric1", "metric2"],
  "confidence": 0.85,
  "details": {{
    "explanation": "Detailed explanation of the business impact",
    "specific_changes": ["change1", "change2"],
    "recommendations": ["recommendation1", "recommendation2"]
  }}
}}

IMPORTANT:
- Be specific with percentages/numbers when possible
- Only flag HIGH severity if business impact is significant
- Focus on stakeholder-facing changes, not internal optimizations
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BUSINESS VALIDATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class BusinessValidator:
    """
    Validates code changes for business impact using LLM analysis.
    
    Uses Google Gemini to:
    - Predict metric shifts
    - Detect metric drift
    - Assess business risk
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = 'gemini-1.5-flash',
        use_llm: bool = False
    ):
        """
        Initialize validator.
        
        Args:
            api_key: Optional LLM API key (only if use_llm=True)
            model: LLM model to use (only if use_llm=True)
            use_llm: Set to True to enable LLM analysis (requires API key)
        """
        self.use_llm = use_llm
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.model_name = model
        self.model = None
        
        # Only initialize LLM if explicitly requested AND API key available
        if use_llm and genai and self.api_key:
            try:
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel(self.model_name)
                logger.info(f"✅ LLM enabled: {self.model_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize LLM: {e}. Using heuristics.")
                self.model = None
        elif use_llm:
            logger.info("⚠️  LLM requested but not available. Using enhanced heuristics.")
    
    def validate(
        self,
        old_code: str,
        new_code: str,
        context: Optional[Dict[str, Any]] = None
    ) -> BusinessValidationReport:
        """
        Perform business validation.
        
        Args:
            old_code: Previous code version
            new_code: New code version
            context: Additional context (table, downstream tables, etc.)
        
        Returns:
            BusinessValidationReport
        """
        context = context or {}
        
        # Use enhanced heuristics by default (fast, no API needed)
        heuristic_report = self._enhanced_heuristic_analysis(old_code, new_code, context)
        
        # If LLM available and enabled, enhance with AI analysis
        if self.model and self.use_llm:
            try:
                return self._llm_enhanced_analysis(old_code, new_code, context, heuristic_report)
            except Exception as e:
                logger.warning(f"LLM analysis failed, using heuristics: {e}")
        
        return heuristic_report
    
    def _llm_enhanced_analysis(
        self,
        old_code: str,
        new_code: str,
        context: Dict[str, Any],
        heuristic_report: BusinessValidationReport
    ) -> BusinessValidationReport:
        """Enhance heuristic analysis with LLM insights."""
        # Build prompt
        prompt = self._build_prompt(old_code, new_code, context)
        
        # Call LLM
        try:
            response = self.model.generate_content(prompt)
            impact = self._parse_llm_response(response.text)
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return self._fallback_analysis(old_code, new_code, context)
        
        # Build report
        risk_level = impact.severity
        summary = impact.predicted_shift
        
        return BusinessValidationReport(
            business_impact=impact.to_dict(),
            risk_level=risk_level,
            summary=summary
        )
    
    def _build_prompt(
        self,
        old_code: str,
        new_code: str,
        context: Dict[str, Any]
    ) -> str:
        """Build LLM prompt from template."""
        return BUSINESS_ANALYSIS_PROMPT.format(
            table_name=context.get('table', 'Unknown'),
            downstream_tables=', '.join(context.get('downstream', [])),
            affected_metrics=', '.join(context.get('affected_metrics', ['Unknown'])),
            old_code=old_code.strip(),
            new_code=new_code.strip()
        )
    
    def _parse_llm_response(self, response_text: str) -> BusinessImpact:
        """Parse LLM JSON response into BusinessImpact."""
        try:
            # Extract JSON from response (LLM might include markdown code blocks)
            json_text = response_text
            if '```json' in response_text:
                json_text = response_text.split('```json')[1].split('```')[0]
            elif '```' in response_text:
                json_text = response_text.split('```')[1].split('```')[0]
            
            data = json.loads(json_text.strip())
            
            return BusinessImpact(
                severity=data.get('severity', 'MEDIUM'),
                predicted_shift=data.get('predicted_shift', 'Unknown impact'),
                metric_drift_detected=data.get('metric_drift_detected', False),
                affected_metrics=data.get('affected_metrics', []),
                confidence=data.get('confidence', 0.5),
                details=data.get('details', {})
            )
        
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {e}")
            # Return fallback
            return BusinessImpact(
                severity='MEDIUM',
                predicted_shift='Unable to parse business impact',
                metric_drift_detected=False,
                affected_metrics=[],
                confidence=0.0,
                details={'error': str(e), 'raw_response': response_text}
            )
    
    def _enhanced_heuristic_analysis(
        self,
        old_code: str,
        new_code: str,
        context: Dict[str, Any]
    ) -> BusinessValidationReport:
        """
        Enhanced heuristic analysis - works without any LLM/API.
        
        Uses pattern matching and code analysis to detect business impact.
        """
        severity = 'LOW'
        predicted_shift = 'No significant business impact detected'
        metric_drift = False
        affected_metrics = []
        details = {
            'method': 'enhanced_heuristics',
            'changes_detected': []
        }
        
        old_lower = (old_code or '').lower()
        new_lower = (new_code or '').lower()
        
        # 1. Filter changes (WHERE clause)
        old_where = 'where' in old_lower
        new_where = 'where' in new_lower
        
        if old_where and not new_where:
            severity = 'MEDIUM'
            predicted_shift = 'Filter removed - data volume will increase, metrics likely to change'
            metric_drift = True
            details['changes_detected'].append('WHERE clause removed')
        elif not old_where and new_where:
            severity = 'MEDIUM'
            predicted_shift = 'Filter added - data volume will decrease, metrics will change'
            metric_drift = True
            details['changes_detected'].append('WHERE clause added')
        elif old_where and new_where:
            # Check if filter logic changed
            old_conditions = re.findall(r'where\s+(.*?)(?:group|order|limit|;|\Z)', old_lower, re.DOTALL)
            new_conditions = re.findall(r'where\s+(.*?)(?:group|order|limit|;|\Z)', new_lower, re.DOTALL)
            if old_conditions != new_conditions:
                severity = 'MEDIUM'
                predicted_shift = 'Filter conditions modified - metrics will shift based on new criteria'
                metric_drift = True
                details['changes_detected'].append('WHERE conditions modified')
        
        # 2. Aggregation changes
        agg_keywords = ['sum(', 'avg(', 'count(', 'max(', 'min(', 'group by']
        old_has_agg = any(kw in old_lower for kw in agg_keywords)
        new_has_agg = any(kw in new_lower for kw in agg_keywords)
        
        if old_has_agg != new_has_agg:
            severity = 'HIGH'
            predicted_shift = 'Aggregation logic changed - significant metric impact expected'
            metric_drift = True
            details['changes_detected'].append('Aggregation logic changed')
        
        # 3. JOIN changes
        old_joins = old_lower.count('join')
        new_joins = new_lower.count('join')
        if old_joins != new_joins:
            severity = max(severity, 'MEDIUM')
            predicted_shift = f'JOIN count changed ({old_joins} → {new_joins}) - data relationships modified'
            metric_drift = True
            details['changes_detected'].append(f'JOIN count changed: {old_joins} → {new_joins}')
        
        # 4. Column calculations
        calc_patterns = [r'\*\s*\d+', r'/\s*\d+', r'\+\s*\d+', r'-\s*\d+']
        old_calcs = sum(len(re.findall(p, old_code or '')) for p in calc_patterns)
        new_calcs = sum(len(re.findall(p, new_code or '')) for p in calc_patterns)
        if old_calcs != new_calcs:
            severity = max(severity, 'MEDIUM')
            predicted_shift = 'Column calculations changed - computed metrics will differ'
            metric_drift = True
            details['changes_detected'].append('Column calculation logic modified')
        
        # Extract affected metrics from context
        if context.get('affected_metrics'):
            affected_metrics = context['affected_metrics']
        
        impact = BusinessImpact(
            severity=severity,
            predicted_shift=predicted_shift,
            metric_drift_detected=metric_drift,
            affected_metrics=affected_metrics,
            confidence=0.7,  # Good confidence for pattern-based analysis
            details=details
        )
        
        return BusinessValidationReport(
            business_impact=impact.to_dict(),
            risk_level=severity,
            summary=predicted_shift
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    # Test with sample SQL
    old_sql = """
    CREATE TABLE silver.orders AS
    SELECT
        order_id,
        customer_id,
        total_amount,
        status
    FROM raw.orders
    WHERE status IN ('active', 'pending')
    """
    
    new_sql = """
    CREATE TABLE silver.orders AS
    SELECT
        order_id,
        customer_id,
        total_amount,
        status
    FROM raw.orders
    WHERE status = 'active'
    """
    
    # Works without any API key - uses smart heuristics
    validator = BusinessValidator()
    report = validator.validate(
        old_code=old_sql,
        new_code=new_sql,
        context={
            'table': 'silver.orders',
            'downstream': ['gold.revenue_dashboard', 'gold.customer_report'],
            'affected_metrics': ['daily_revenue', 'order_count']
        }
    )
    
    print("\n" + "="*70)
    print("BUSINESS VALIDATION RESULTS")
    print("="*70)
    print(json.dumps(report.to_dict(), indent=2))
