"""Exercise the actual MCP wrapper against the API request contract."""
import ast
import json
from pathlib import Path
from unittest.mock import Mock


def test_comparison_payload_matches_api_contract():
    root = Path(__file__).resolve().parents[2]
    tree = ast.parse((root / 'mcp-server/server.py').read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'compare_trade')
    fn.decorator_list = []
    http = Mock()
    http.post.return_value.json.return_value = {'comparable': True}
    scope = {'Context': object, 'httpx': http, 'json': json,
             'API_BASE': 'http://test', '_resolve_token': lambda ctx: 'test'}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), '<comparison>', 'exec'), scope)
    result = json.loads(scope['compare_trade'](['IN', 'CN'], 'ex', tnved4=['2709']))
    assert result['comparable']
    payload = http.post.call_args.kwargs['json']
    api = ast.parse((root / 'api/app/routers/comparison.py').read_text(encoding='utf-8'))
    schema = next(n for n in api.body if isinstance(n, ast.ClassDef) and n.name == 'ComparisonQuery')
    fields = {n.target.id for n in schema.body if isinstance(n, ast.AnnAssign)}
    assert set(payload) <= fields
    assert payload['strana'] == ['IN', 'CN']
    assert payload['tnved4'] == ['2709']
    assert payload['include_fizob'] is True
