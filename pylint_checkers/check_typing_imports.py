"""
A pylint checker to check that deprecated imports are not being
taken from `typing`.

No longer in use since downgrade to Python 3.7.
"""

from typing import TYPE_CHECKING

from astroid.node_classes import Attribute, Import, ImportFrom, Name
from pylint.checkers import BaseChecker

if TYPE_CHECKING:
    from pylint.lint import PyLinter


DEPRECATED_REPLACEMENTS: dict[str, str] = {
    "Tuple": "tuple",
    "Callable": "collections.abc.Callable",
    "Type": "type",
    "Dict": "dict",
    "List": "list",
    "Set": "set",
    "FrozenSet": "frozenset",
    "DefaultDict": "collections.defaultdict",
    "OrderedDict": "collections.OrderedDict",
    "ChainMap": "collections.ChainMap",
    "Counter": "collections.counter",
    "Deque": "collections.deque",
    "Pattern": "re.Pattern",
    "Match": "re.Match",
    "AbstractSet": "collections.abc.AbstractSet",
    "ByteString": "collections.abc.ByteString",
    "Collection": "collections.abc.Collection",
    "Container": "collections.abc.Container",
    "ItemsView": "collections.abc.ItemsView",
    "KeysView": "collections.abc.KeysView",
    "Mapping": "collections.abc.Mapping",
    "MappingView": "collections.abc.MappingView",
    "MutableMapping": "collections.abc.MutableMapping",
    "MutableSequence": "collections.abc.MutableSequence",
    "MutableSet": "collections.abc.MutableSet",
    "Sequence": "collections.abc.Sequence",
    "ValuesView": "collections.abc.ValuesView",
    "Iterable": "collections.abc.Iterable",
    "Iterator": "collections.abc.Iterator",
    "Generator": "collections.abc.Generator",
    "Hashable": "collections.abc.Hashable",
    "Reversible": "collections.abc.Reversible",
    "Sized": "collections.abc.Sized",
    "Coroutine": "collections.abc.Coroutine",
    "AsyncGenerator": "collections.abc.AsyncGenerator",
    "AsyncIterable": "collections.abc.AsyncIterable",
    "AsyncIterator": "collections.abc.AsyncIterator",
    "Awaitable": "collections.abc.Awaitable",
    "ContextManager": "contextlib.AbstractContextManager",
    "AsyncContextManager": "contextlib.AbstractAsyncContextManager",
}
"""Deprecated typing imports and their replacements."""


class TypingImportChecker(BaseChecker):
    """A pylint 'checker' to validate that deprecated typing generics are not being used."""

    name = "deprecated-typing-imports-checker"
    msgs = {
        "W1901": (
            "Using deprecated 'typing.%s' instead of %s'%s'",
            "deprecated-typing-generic",
            "Emitted when deprecated typing generics are used instead of their replacements.",
            {"minversion": (3, 9)},
        ),
    }

    def visit_import(self, node: Import):
        """Check import usage."""
        for import_name, import_alias in node.names:
            if import_name != "typing":
                continue

            typing_name = import_alias or import_name
            for attr_node in node.root().nodes_of_class(Attribute):
                name_node = attr_node.expr

                if not isinstance(name_node, Name) or name_node.name != typing_name:
                    continue

                name = attr_node.attrname
                replacement = DEPRECATED_REPLACEMENTS.get(name)
                qualifier = "builtin " if "." not in replacement else ""
                self.add_message(
                    "deprecated-typing-generic",
                    node=attr_node,
                    args=(name, qualifier, replacement),
                )

    def visit_importfrom(self, node: ImportFrom):
        """Check import from usage."""
        if not node.modname == "typing":
            return

        for name, _ in node.names:
            replacement = DEPRECATED_REPLACEMENTS.get(name)
            if replacement is not None:
                qualifier = "builtin " if "." not in replacement else ""
                self.add_message(
                    "deprecated-typing-generic", node=node, args=(name, qualifier, replacement)
                )


def register(linter: "PyLinter"):
    """Register the checker."""
    linter.register_checker(TypingImportChecker(linter))
