"""
╔══════════════════════════════════════════════════════════════════╗
║                  PEER REVIEW ORCHESTRATOR                        ║
╚══════════════════════════════════════════════════════════════════╝

Main orchestrator that synthesizes all validations into a commit advisory.

USAGE:
    from peer_review import PeerReviewOrchestrator
    
    orchestrator = PeerReviewOrchestrator()
    advisory = orchestrator.review_changes()
    
    print(advisory.formatted_output)
"""

import sys
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.peer_review.semantic_delta import SemanticDeltaExtractor
from scripts.peer_review.blast_radius import BlastRadiusMapper
from scripts.peer_review.technical_validator import TechnicalValidator
from scripts.peer_review.business_validator import BusinessValidator
from scripts.debug_engine import DebugEngine
from config.db_config import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA STRUCTURES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class CommitAdvisory:
    """Final commit advisory with all analysis results."""
    risk_level: str  # 'GREEN', 'YELLOW', 'RED'
    advisory: str  # 'Safe to commit', 'Proceed with caution', 'Manual review required'
    
    # Component results
    semantic_delta: Dict[str, Any]
    blast_radius: Dict[str, Any]
    technical_report: Dict[str, Any]
    business_report: Dict[str, Any]
    
    # Summary
    files_changed: int
    technical_blockers: int
    business_impact_summary: str
    
    # Formatted output
    formatted_output: str
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PEER REVIEW ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PeerReviewOrchestrator:
    """
    Main orchestrator for the Senior Peer Review system.
    
    Coordinates all validation components and generates final advisory.
    """
    
    def __init__(
        self,
        repo_path: Optional[str] = None,
        gemini_api_key: Optional[str] = None
    ):
        """
        Initialize orchestrator.
        
        Args:
            repo_path: Path to git repository
            gemini_api_key: Gemini API key for business validation
        """
        # Initialize debug engine
        try:
            self.debug_engine = DebugEngine()
        except Exception as e:
            logger.warning(f"Could not initialize DebugEngine: {e}")
            self.debug_engine = None
        
        # Initialize components
        self.delta_extractor = SemanticDeltaExtractor(repo_path)
        self.blast_mapper = BlastRadiusMapper(self.debug_engine)
        self.technical_validator = TechnicalValidator(self.debug_engine)
        self.business_validator = BusinessValidator(api_key=gemini_api_key)
    
    def review_changes(self, staged_only: bool = True) -> CommitAdvisory:
        """
        Review staged changes and generate commit advisory.
        
        Args:
            staged_only: Only review staged files (True) or all modified (False)
        
        Returns:
            CommitAdvisory with complete analysis
        """
        logger.info("="*70)
        logger.info("STARTING SENIOR PEER REVIEW")
        logger.info("="*70)
        
        # Suppress verbose logging during execution
        import logging
        logging.getLogger('databricks').setLevel(logging.ERROR)
        logging.getLogger('scripts.peer_review').setLevel(logging.ERROR)
        logging.getLogger('DebugEngine').setLevel(logging.ERROR)
        
        # Step 1: Extract semantic delta
        delta = self.delta_extractor.extract_from_git(staged_only=staged_only)
        
        # If no changes, return early
        if not delta.modified_elements:
            return self._create_no_changes_advisory()
        
        # Step 2: Map blast radius
        blast_radius = self.blast_mapper.map_impact(delta.modified_elements)
        
        # Extract impacted table names
        impacted_tables = [node['table'] for node in blast_radius.impacted_nodes]
        
        # Step 3: Technical validation
        
        # Get old/new code from first changed file
        old_code, new_code = self._get_code_samples(delta)
        
        technical_report = self.technical_validator.validate(
            modified_elements=delta.modified_elements,
            impacted_nodes=impacted_tables,
            old_code=old_code,
            new_code=new_code
        )
        
        # Step 4: Business validation
        
        business_report = self.business_validator.validate(
            old_code=old_code,
            new_code=new_code,
            context={
                'table': delta.modified_elements[0] if delta.modified_elements else 'Unknown',
                'downstream': impacted_tables[:5],  # Top 5 impacted
                'affected_metrics': self._extract_metrics(impacted_tables)
            }
        )
        
        # Step 5: Synthesize advisory
        advisory = self._synthesize_advisory(
            delta=delta,
            blast_radius=blast_radius,
            technical_report=technical_report,
            business_report=business_report
        )
        
        return advisory
    
    def _get_code_samples(self, delta) -> tuple:
        """Extract old and new code samples from delta details."""
        if not delta.details:
            return "", ""
        
        # Get first file's details
        first_file = list(delta.details.keys())[0]
        
        # Read the actual current file content
        try:
            file_path = Path(first_file)
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    new_code = f.read()
            else:
                new_code = ""
        except Exception as e:
            logger.debug(f"Could not read file {first_file}: {e}")
            new_code = ""
        
        # Try to get old version from git
        try:
            import subprocess
            result = subprocess.run(
                ['git', 'show', f'HEAD:{first_file}'],
                capture_output=True,
                text=True,
                cwd=Path(first_file).parent if Path(first_file).parent.exists() else '.'
            )
            old_code = result.stdout if result.returncode == 0 else ""
        except Exception as e:
            logger.debug(f"Could not get old version from git: {e}")
            old_code = ""
        
        return old_code, new_code
    
    def _extract_metrics(self, impacted_tables: list) -> list:
        """Extract likely metric names from impacted tables."""
        metrics = []
        metric_keywords = ['revenue', 'sales', 'count', 'total', 'average', 'dashboard', 'report']
        
        for table in impacted_tables:
            table_lower = table.lower()
            for keyword in metric_keywords:
                if keyword in table_lower:
                    metrics.append(table)
                    break
        
        return metrics[:3]  # Return top 3
    
    def _create_no_changes_advisory(self) -> CommitAdvisory:
        """Create advisory when no changes detected."""
        return CommitAdvisory(
            risk_level='GREEN',
            advisory='Safe to commit',
            semantic_delta={},
            blast_radius={},
            technical_report={},
            business_report={},
            files_changed=0,
            technical_blockers=0,
            business_impact_summary='No changes detected',
            formatted_output=self._format_no_changes()
        )
    
    def _synthesize_advisory(
        self,
        delta,
        blast_radius,
        technical_report,
        business_report
    ) -> CommitAdvisory:
        """Synthesize all results into final advisory."""
        
        # Determine overall risk level
        risk_level = self._calculate_risk_level(
            delta=delta,
            technical_report=technical_report,
            business_report=business_report,
            blast_radius=blast_radius
        )
        
        # Determine advisory message
        advisory = self._get_advisory_message(risk_level)
        
        # Count technical blockers
        technical_blockers = len(technical_report.technical_blockers)
        
        # Get business impact summary
        business_impact_summary = business_report.summary
        
        # Generate formatted output
        formatted_output = self._format_advisory(
            risk_level=risk_level,
            advisory=advisory,
            delta=delta,
            blast_radius=blast_radius,
            technical_report=technical_report,
            business_report=business_report
        )
        
        return CommitAdvisory(
            risk_level=risk_level,
            advisory=advisory,
            semantic_delta=delta.to_dict(),
            blast_radius=blast_radius.to_dict(),
            technical_report=technical_report.to_dict(),
            business_report=business_report.to_dict(),
            files_changed=len(delta.details),
            technical_blockers=technical_blockers,
            business_impact_summary=business_impact_summary,
            formatted_output=formatted_output
        )
    
    def _calculate_risk_level(
        self,
        delta,
        technical_report,
        business_report,
        blast_radius
    ) -> str:
        """
        Calculate overall risk level.
        
        Returns: 'GREEN', 'YELLOW', or 'RED'
        """
        # RED if:
        # - High severity technical blockers
        # - High business impact + integrity flag
        # - Critical leaf nodes impacted
        
        if technical_report.risk_level == 'HIGH':
            return 'RED'
        
        if business_report.risk_level == 'HIGH' and delta.integrity_flag:
            return 'RED'
        
        high_criticality_count = blast_radius.criticality_breakdown.get('HIGH', 0)
        if high_criticality_count >= 3:  # 3+ high criticality nodes
            return 'RED'
        
        # YELLOW if:
        # - Medium technical issues
        # - Medium business impact
        # - Some high criticality nodes
        
        if technical_report.risk_level in ['MEDIUM', 'HIGH']:
            return 'YELLOW'
        
        if business_report.risk_level == 'MEDIUM':
            return 'YELLOW'
        
        if high_criticality_count > 0:
            return 'YELLOW'
        
        # Otherwise GREEN
        return 'GREEN'
    
    def _get_advisory_message(self, risk_level: str) -> str:
        """Get advisory message based on risk level."""
        if risk_level == 'GREEN':
            return 'Safe to commit'
        elif risk_level == 'YELLOW':
            return 'Proceed with caution'
        else:  # RED
            return 'Manual review required'
    
    def _format_advisory(
        self,
        risk_level: str,
        advisory: str,
        delta,
        blast_radius,
        technical_report,
        business_report
    ) -> str:
        """Format advisory for terminal output."""
        
        # Map risk level to emoji
        risk_emoji = {
            'GREEN': '🟢',
            'YELLOW': '🟡',
            'RED': '🔴'
        }
        
        output = []
        output.append("")
        output.append("╔" + "="*68 + "╗")
        output.append("║" + " "*20 + "SENIOR PEER REVIEW" + " "*30 + "║")
        output.append("╚" + "="*68 + "╝")
        output.append("")
        
        output.append(f"Risk Level: {risk_emoji.get(risk_level, '⚪')} {risk_level} - {advisory}")
        output.append("")
        
        # Files changed
        output.append("📋 Files Changed (Direct Edits):")
        output.append("   These are the files you touched directly.")
        for filename in delta.details.keys():
            output.append(f"   • {filename}")
        output.append("")
        
        # Detailed passively impacted files (downstream dependencies)
        if blast_radius.impacted_nodes:
            output.append("🔗 Indirectly Impacted Files (Downstream Impact Chain):")
            output.append("   These files didn't change, but they USE data from the files you changed — like dominoes!")
            output.append("")
            
            # Group by distance (hop count) to show impact propagation
            by_distance = {}
            for node in blast_radius.impacted_nodes:
                dist = node['distance']
                if dist not in by_distance:
                    by_distance[dist] = []
                by_distance[dist].append(node)
            
            # Show impact propagation level by level
            for distance in sorted(by_distance.keys()):
                nodes_at_distance = by_distance[distance]
                
                if distance == 1:
                    output.append(f"   📍 Level {distance} - Direct Dependencies ({len(nodes_at_distance)} table(s)):")
                else:
                    output.append(f"   📍 Level {distance} - {distance} Hops Away ({len(nodes_at_distance)} table(s)):")
                
                # Group by criticality at this level
                high = [n for n in nodes_at_distance if n['criticality'] == 'HIGH']
                medium = [n for n in nodes_at_distance if n['criticality'] == 'MEDIUM']
                low = [n for n in nodes_at_distance if n['criticality'] == 'LOW']
                
                for node in high[:3]:
                    output.append(f"      🔴 {node['table']}")
                    output.append(f"         Type: {node['node_type']}")
                    if node.get('file_path'):
                        output.append(f"         File: {node['file_path']}")
                
                for node in medium[:3]:
                    output.append(f"      🟡 {node['table']}")
                    output.append(f"         Type: {node['node_type']}")
                    if node.get('file_path'):
                        output.append(f"         File: {node['file_path']}")
                
                for node in low[:2]:
                    output.append(f"      🟢 {node['table']}")
                    output.append(f"         Type: {node['node_type']}")
                    if node.get('file_path'):
                        output.append(f"         File: {node['file_path']}")
                
                # Show count if truncated
                shown = min(3, len(high)) + min(3, len(medium)) + min(2, len(low))
                if len(nodes_at_distance) > shown:
                    output.append(f"      ... and {len(nodes_at_distance) - shown} more at this level")
                
                output.append("")
            
            # Summary
            total_impacted = len(blast_radius.impacted_nodes)
            leaf_count = len(blast_radius.leaf_nodes)
            output.append(f"   📊 Impact Summary:")
            output.append(f"      • Total impacted: {total_impacted} table(s)")
            output.append(f"      • Critical dashboards/reports: {leaf_count}")
            output.append(f"      • Cascade depth: {max(by_distance.keys())} level(s)")
            output.append("")
        else:
            output.append("🔗 Indirectly Impacted Files:")
            output.append("   These files didn't change, but they USE data from the files you changed — like dominoes!")
            output.append("   ✅ No downstream dependencies detected")
            output.append("   (Run 'python scripts/cli.py build' to build lineage metadata)")
            output.append("")
        
        # Technical blockers
        if technical_report.technical_blockers:
            output.append("⚠️  Technical Blockers:")
            output.append("   These are bugs or mistakes that could BREAK things if you deploy this code.")
            for blocker in technical_report.technical_blockers:
                severity = blocker['severity']
                message = blocker['message']
                output.append(f"   • [{severity}] {message}")
            output.append("")
        
        # Business impact
        if business_report.business_impact:
            impact = business_report.business_impact
            output.append("📊 Business Impact:")
            output.append("   This is how your change might affect the numbers people see in reports and dashboards.")
            output.append(f"   • {impact.get('predicted_shift', 'Unknown impact')}")
            if impact.get('metric_drift_detected'):
                output.append(f"   • Metric drift detected in {len(impact.get('affected_metrics', []))} metric(s)")
            output.append("")
        
        # Blast radius
        output.append("🔍 Blast Radius:")
        output.append("   This shows HOW FAR your change ripples through the system — like throwing a stone in water.")
        output.append(f"   • {len(blast_radius.impacted_nodes)} downstream table(s) affected")
        output.append(f"   • {len(blast_radius.leaf_nodes)} dashboard/report(s) impacted")
        if blast_radius.criticality_breakdown:
            crit = blast_radius.criticality_breakdown
            output.append(f"   • Criticality: {crit.get('HIGH', 0)} HIGH, {crit.get('MEDIUM', 0)} MEDIUM, {crit.get('LOW', 0)} LOW")
        output.append("")
        
        # Advisory
        output.append("💡 Advisory:")
        output.append("   This is the final recommendation — what you should do next.")
        if risk_level == 'RED':
            output.append("   " + advisory.upper())
            output.append("")
            output.append("   Recommended actions:")
            output.append("   1. Review technical blockers and fix issues")
            output.append("   2. Validate business impact with stakeholders")
            output.append("   3. Test changes in staging environment")
            output.append("   4. Update documentation if metrics changed")
        elif risk_level == 'YELLOW':
            output.append("   " + advisory)
            output.append("")
            output.append("   Recommended actions:")
            output.append("   1. Review warnings carefully")
            output.append("   2. Consider adding tests for affected areas")
            output.append("   3. Monitor metrics after deployment")
        else:
            output.append("   ✅ " + advisory)
            output.append("   No significant issues detected")
        
        output.append("")
        output.append("─" * 70)
        
        return "\n".join(output)
    
    def _format_no_changes(self) -> str:
        """Format output when no changes detected."""
        return """
╔══════════════════════════════════════════════════════════════════╗
║                    SENIOR PEER REVIEW                            ║
╚══════════════════════════════════════════════════════════════════╝

Risk Level: 🟢 GREEN - Safe to commit

No significant code changes detected in staged files.

──────────────────────────────────────────────────────────────────
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI / Testing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    import json
    
    # Test orchestrator
    orchestrator = PeerReviewOrchestrator()
    advisory = orchestrator.review_changes(staged_only=True)
    
    # Print formatted output
    print(advisory.formatted_output)
    
    # Also save JSON for debugging
    with open('peer_review_result.json', 'w') as f:
        json.dump(advisory.to_dict(), f, indent=2)
    
    print("\nDetailed results saved to peer_review_result.json")
