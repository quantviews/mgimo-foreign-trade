"""The XLSX summary must not replace a matched-period comparison."""
import ast
import asyncio
from contextvars import ContextVar
from pathlib import Path


def test_preserve_verified_comparison_after_export():
    source = Path(__file__).resolve().parents[1] / 'app/deepseek.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == '_checked_answer')
    context = ContextVar('test_export', default={
        'verified_comparison_answer': 'Verified common 11-month comparison',
        'numeric_facts': {'export': {'kind': 'total', 'text': 'Wider 12-month total'}},
    })
    scope = {'export_context': context, '_answer': lambda msg: msg['content']}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), '<answer>', 'exec'), scope)
    actual = asyncio.run(scope['_checked_answer'](None, [], {'content': 'Unverified model total'}))
    assert actual == 'Verified common 11-month comparison'
