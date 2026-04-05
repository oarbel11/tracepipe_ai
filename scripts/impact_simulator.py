import networkx as nx
import json
from typing import Dict, List, Any, Optional
from .peer_review.blast_radius import ImpactAnalysisMapper


class ImpactSimulator:
    def __init__(self, config_path: Optional[str] = None):
        self.mapper = ImpactAnalysisMapper()
        self.graph = nx.DiGraph()
        self.simulation_results = {}

    def simulate_change(self, target: str, change_type: str, 
                       change_params: Dict[str, Any] = None) -> Dict:
        change_params = change_params or {}
        
        impact_data = self.mapper.get_downstream_impact(target)
        
        simulation = {
            'target': target,
            'change_type': change_type,
            'change_params': change_params,
            'affected_assets': [],
            'affected_count': 0,
            'risk_score': 0.0,
            'graph_data': {'nodes': [], 'edges': []}
        }
        
        self._build_graph(target, impact_data)
        affected = self._analyze_downstream(target, change_type, change_params)
        
        simulation['affected_assets'] = affected
        simulation['affected_count'] = len(affected)
        simulation['risk_score'] = self._calculate_risk(affected, change_type)
        simulation['graph_data'] = self._export_graph()
        
        self.simulation_results = simulation
        return simulation

    def _build_graph(self, root: str, impact_data: Dict):
        self.graph.clear()
        self.graph.add_node(root, type='source', risk=0.0)
        
        for asset_type, assets in impact_data.items():
            for asset in assets:
                asset_id = f"{asset_type}:{asset.get('name', asset)}"
                self.graph.add_node(asset_id, type=asset_type, risk=0.0)
                self.graph.add_edge(root, asset_id)

    def _analyze_downstream(self, target: str, change_type: str, 
                           params: Dict) -> List[Dict]:
        affected = []
        risk_map = {'drop_column': 0.8, 'schema_change': 0.6, 
                   'deprecate': 0.9, 'data_quality': 0.5}
        base_risk = risk_map.get(change_type, 0.5)
        
        for node in nx.descendants(self.graph, target):
            node_type = self.graph.nodes[node].get('type', 'unknown')
            risk = base_risk * (1.2 if node_type == 'ml_model' else 1.0)
            
            affected.append({
                'asset': node,
                'type': node_type,
                'risk_score': min(risk, 1.0),
                'distance': nx.shortest_path_length(self.graph, target, node)
            })
            self.graph.nodes[node]['risk'] = min(risk, 1.0)
        
        return sorted(affected, key=lambda x: x['risk_score'], reverse=True)

    def _calculate_risk(self, affected: List[Dict], change_type: str) -> float:
        if not affected:
            return 0.0
        total_risk = sum(a['risk_score'] for a in affected)
        return min(total_risk / len(affected), 1.0)

    def _export_graph(self) -> Dict:
        nodes = [{'id': n, **self.graph.nodes[n]} for n in self.graph.nodes()]
        edges = [{'source': u, 'target': v} for u, v in self.graph.edges()]
        return {'nodes': nodes, 'edges': edges}

    def visualize_graph(self, simulation: Dict, output: str = 'impact.html'):
        html = self._generate_html(simulation)
        with open(output, 'w') as f:
            f.write(html)
        return output

    def _generate_html(self, sim: Dict) -> str:
        nodes_json = json.dumps(sim['graph_data']['nodes'])
        edges_json = json.dumps(sim['graph_data']['edges'])
        
        return f'''<!DOCTYPE html>
<html><head><title>Impact Analysis</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>body{{font-family:sans-serif}}.node{{cursor:pointer}}
.high-risk{{fill:red}}.medium-risk{{fill:orange}}.low-risk{{fill:green}}</style>
</head><body>
<h1>Impact Simulation: {sim['target']}</h1>
<p>Change: {sim['change_type']} | Risk: {sim['risk_score']:.2f} | Affected: {sim['affected_count']}</p>
<div id="graph"></div>
<script>
const nodes={nodes_json};
const links={edges_json};
const width=960,height=600;
const svg=d3.select('#graph').append('svg').attr('width',width).attr('height',height);
const simulation=d3.forceSimulation(nodes).force('link',d3.forceLink(links).id(d=>d.id))
.force('charge',d3.forceManyBody().strength(-300)).force('center',d3.forceCenter(width/2,height/2));
const link=svg.append('g').selectAll('line').data(links).enter().append('line')
.attr('stroke','#999').attr('stroke-width',2);
const node=svg.append('g').selectAll('circle').data(nodes).enter().append('circle')
.attr('r',d=>d.type==='source'?15:10)
.attr('class',d=>d.risk>0.7?'high-risk':d.risk>0.4?'medium-risk':'low-risk')
.call(d3.drag().on('start',dragstarted).on('drag',dragged).on('end',dragended));
node.append('title').text(d=>d.id+' (risk: '+d.risk.toFixed(2)+')');
simulation.on('tick',()=>{{link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
.attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
node.attr('cx',d=>d.x).attr('cy',d=>d.y);}});
function dragstarted(e){{if(!e.active)simulation.alphaTarget(0.3).restart();e.subject.fx=e.subject.x;e.subject.fy=e.subject.y;}}
function dragged(e){{e.subject.fx=e.x;e.subject.fy=e.y;}}
function dragended(e){{if(!e.active)simulation.alphaTarget(0);e.subject.fx=null;e.subject.fy=null;}}
</script></body></html>'''
