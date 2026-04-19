import json
from typing import Dict, List, Any
from pathlib import Path

class CrossWorkspaceLineage:
    def __init__(self, config: Dict[str, Any]):
        self.workspaces = config.get('workspaces', [])
        self.lineage_cache = {}

    def aggregate_lineage(self) -> Dict[str, Any]:
        """Aggregate lineage from multiple workspaces."""
        aggregated = {
            'workspaces': {},
            'cross_workspace_flows': []
        }
        for workspace in self.workspaces:
            workspace_id = workspace.get('id', '')
            aggregated['workspaces'][workspace_id] = self._fetch_workspace_lineage(workspace)
        aggregated['cross_workspace_flows'] = self._identify_cross_flows(aggregated['workspaces'])
        return aggregated

    def _fetch_workspace_lineage(self, workspace: Dict) -> Dict[str, Any]:
        """Fetch lineage data from a single workspace."""
        workspace_id = workspace.get('id', '')
        if workspace_id in self.lineage_cache:
            return self.lineage_cache[workspace_id]
        lineage = {
            'workspace_id': workspace_id,
            'notebooks': workspace.get('notebooks', []),
            'tables': workspace.get('tables', [])
        }
        self.lineage_cache[workspace_id] = lineage
        return lineage

    def _identify_cross_flows(self, workspaces_data: Dict) -> List[Dict[str, Any]]:
        """Identify data flows across workspaces."""
        flows = []
        for ws1_id, ws1_data in workspaces_data.items():
            for ws2_id, ws2_data in workspaces_data.items():
                if ws1_id != ws2_id:
                    flow = self._find_flow(ws1_data, ws2_data, ws1_id, ws2_id)
                    if flow:
                        flows.append(flow)
        return flows

    def _find_flow(self, src_ws: Dict, tgt_ws: Dict, src_id: str, tgt_id: str) -> Dict:
        """Find flow between two workspaces."""
        src_tables = set(src_ws.get('tables', []))
        tgt_tables = set(tgt_ws.get('tables', []))
        common = src_tables.intersection(tgt_tables)
        if common:
            return {
                'source_workspace': src_id,
                'target_workspace': tgt_id,
                'shared_tables': list(common)
            }
        return None

    def query_lineage(self, notebook_path: str) -> Dict[str, Any]:
        """Query lineage for a specific notebook across workspaces."""
        results = []
        for workspace in self.workspaces:
            ws_data = self._fetch_workspace_lineage(workspace)
            if notebook_path in ws_data.get('notebooks', []):
                results.append(ws_data)
        return {'notebook': notebook_path, 'found_in': results}
