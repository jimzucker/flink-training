"""Undefined-name check with no dependency, using Python's own scope analysis.

A name a function reads that is neither local, nor free from an enclosing
function, nor a module global, nor a builtin, is a defect that only shows up
when that line runs. One reached the rig on 2026-09-08: preflight's host probe
wrote to `out`, a name that exists in a different function, and the chain
failed at preflight after the pure self-test and the replay had both passed --
neither runs preflight, which needs Docker.
"""
import builtins
import symtable
import sys


def undefined(path):
    src = open(path).read()
    top = symtable.symtable(src, path, "exec")
    module_names = set(top.get_identifiers()) | set(dir(builtins))
    bad = []

    def walk(table, enclosing):
        names = set(table.get_identifiers()) | enclosing
        for sym in table.get_symbols():
            if sym.is_global() and not sym.is_assigned() and sym.get_name() not in module_names:
                bad.append((table.get_lineno(), table.get_name(), sym.get_name()))
        for child in table.get_children():
            walk(child, names)

    for child in top.get_children():
        walk(child, module_names)
    return bad


if __name__ == "__main__":
    found = []
    for p in sys.argv[1:]:
        found += [(p,) + b for b in undefined(p)]
    for p, line, scope, name in found:
        print(f"{p}:{line}: {scope}() reads undefined name {name!r}")
    print(f"namecheck: {len(found)} undefined name(s) in {len(sys.argv) - 1} file(s)")
    sys.exit(1 if found else 0)
