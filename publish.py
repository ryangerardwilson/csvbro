#!/usr/bin/env python3
import os
import re
import sys
import ast
import json
import pkgutil
import subprocess
import importlib.util
from shutil import rmtree, copytree, copyfile


def get_standard_library_modules():
    """Return a set of stdlib module names (excluding site-packages)."""
    stdlib = set()
    for mod in pkgutil.iter_modules():
        if mod.ispkg or mod.name.startswith('_'):
            continue
        try:
            spec = importlib.util.find_spec(mod.name)
            if spec and spec.origin and 'site-packages' not in spec.origin:
                stdlib.add(mod.name)
        except Exception:
            pass
    stdlib.update({
        'builtins', 'sqlite3', 'sys', 'os', 'math', 'time', 'datetime', 'random',
        'json', 're', 'string', 'collections', 'itertools', 'functools', 'shutil',
        'subprocess', 'pathlib', 'configparser', 'io', 'types', 'warnings', 'abc'
    })
    return stdlib


def infer_dependencies(app_dir, project_name):
    """
    Parse all .py files under app_dir for imports, then remove any modules
    that are internal, part of the standard library, or the project itself.
    """
    stdlib = get_standard_library_modules()
    deps = set()
    internal_modules = set()

    for root, dirs, files in os.walk(app_dir):
        for fn in files:
            if fn.endswith('.py') and fn != '__init__.py':
                module_name = fn[:-3]
                internal_modules.add(module_name)
        for dir_name in dirs:
            if os.path.exists(os.path.join(root, dir_name, '__init__.py')):
                internal_modules.add(dir_name)

    for root, _, files in os.walk(app_dir):
        for fn in files:
            if not fn.endswith('.py'):
                continue
            path = os.path.join(root, fn)
            text = open(path, 'r', encoding='utf-8').read()
            try:
                tree = ast.parse(text)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for n in node.names:
                            pkg = n.name.split('.')[0]
                            if pkg not in stdlib and pkg not in internal_modules and pkg != project_name:
                                deps.add(pkg)
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        pkg = node.module.split('.')[0]
                        if pkg not in stdlib and pkg not in internal_modules and pkg != project_name:
                            deps.add(pkg)
            except SyntaxError:
                for m in re.findall(r'^\s*(?:import|from)\s+([A-Za-z0-9_]+)',
                                    text, re.MULTILINE):
                    if m not in stdlib and m not in internal_modules and m != project_name:
                        deps.add(m)

    print(f"Detected external dependencies: {sorted(deps)}")
    print(f"Excluded internal modules/packages: {sorted(internal_modules)}")

    return sorted(deps)


def read_local_version():
    """Read version = "X.Y.Z" from pyproject.toml, or return None."""
    fn = "pyproject.toml"
    if not os.path.exists(fn):
        return None
    txt = open(fn, 'r', encoding='utf-8').read()
    m = re.search(r'version\s*=\s*"(\d+)\.(\d+)\.(\d+)"', txt)
    return tuple(map(int, m.groups())) if m else None


def read_pypi_version(name):
    """Fetch version tuple from PyPI JSON, or None on error."""
    try:
        import urllib.request
        url = f"https://pypi.org/pypi/{name}/json"
        resp = urllib.request.urlopen(url, timeout=5)
        info = json.loads(resp.read().decode())
        return tuple(map(int, info['info']['version'].split('.')))
    except Exception:
        return None


def bump_patch(vt):
    """Given (X,Y,Z) return 'X.Y.(Z+1)'."""
    return f"{vt[0]}.{vt[1]}.{vt[2] + 1}"


def determine_new_version(name):
    """
    Compare local version vs PyPI version, pick the higher,
    bump its patch number, or start at 0.0.1.
    """
    local = read_local_version()
    remote = read_pypi_version(name)
    if not local and not remote:
        return "0.0.1"
    if remote is None or (local and local >= remote):
        base = local
    else:
        base = remote
    return bump_patch(base)


def detect_app_structure(app_dir):
    """Detect if app/ has a modules/ subdirectory or only .py files."""
    has_modules = os.path.exists(os.path.join(app_dir, "modules"))
    py_files = [f[:-3] for f in os.listdir(app_dir) if f.endswith('.py') and f != '__init__.py']
    return has_modules, py_files


def patch_imports(app_dir, project_name, has_modules, py_files):
    """Patch .py files based on app structure."""
    print(f"Patching imports in {app_dir} for {project_name}")
    for root, _, files in os.walk(app_dir):
        for fn in files:
            if not fn.endswith('.py'):
                continue
            path = os.path.join(root, fn)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            print(f"Before patching {path}:\n{content[:500]}...")
            # Replace imports
            new_content = content
            if has_modules and os.path.basename(root) == 'modules':
                # In modules/, no patching needed for module imports
                pass
            elif has_modules:
                # In app/, patch modules imports
                new_content = re.sub(r'\bfrom modules\.', f'from {project_name}.app.modules.', content)
            else:
                # No modules/, patch sibling .py imports
                for py_file in py_files:
                    new_content = re.sub(rf'\bfrom {py_file}\b', f'from {project_name}.app.{py_file}', new_content)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            with open(path, 'r', encoding='utf-8') as f:
                patched_content = f.read()
            print(f"After patching {path}:\n{patched_content[:500]}...")
    return []


def write_configs(name, deps, version, has_modules):
    """Rewrite pyproject.toml + setup.cfg based on app structure."""
    dep_block = ""
    if deps:
        dep_block = "\n".join(f'  "{d}",' for d in deps)

    py = f"""[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{name}"
version = "{version}"
description = "A tool for browsing and manipulating CSV files"
readme = "README.md"
requires-python = ">=3.10"
license = "MIT"

authors = [
  {{ name = "Ryan Gerard Wilson", email = "ryan@wilsonfamilyoffice.com" }}
]

dependencies = [
{dep_block}
]

classifiers = [
  "Programming Language :: Python :: 3",
  "Operating System :: OS Independent"
]

[project.urls]
Homepage = "https://ryangerardwilson.com"
Issues   = "https://ryangerardwilson.com/contact"

[project.entry-points."console_scripts"]
{name} = "{name}.app.main:main"
"""
    open("pyproject.toml", "w", encoding="utf-8").write(py)

    if has_modules:
        packages = f"{name}.app, {name}.app.modules"
        package_dir = f"""    {name}.app = {name}/app
    {name}.app.modules = {name}/app/modules"""
    else:
        packages = f"{name}.app"
        package_dir = f"    {name}.app = {name}/app"

    cfg = f"""[metadata]
long_description = file: README.md
long_description_content_type = text/markdown

[options]
packages = {packages}
package_dir =
{package_dir}
python_requires = >=3.10
include_package_data = True
"""
    open("setup.cfg", "w", encoding="utf-8").write(cfg)

    # Create MANIFEST.in to include all .py files
    manifest = f"""include LICENSE
include README.md
recursive-include {name}/app *.py
"""
    # Make README.md optional
    if not os.path.exists("README.md"):
        manifest = manifest.replace("include README.md\n", "")
    open("MANIFEST.in", "w", encoding="utf-8").write(manifest)

    print(f"→ Wrote pyproject.toml, setup.cfg, and MANIFEST.in (version = {version})")

def prepare_build(name):
    """Copy app/ to <project_name>/app/ and create __init__.py only if missing."""
    if not os.path.exists("app"):
        print(f"Error: app/ directory not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Preparing build: Copying app/ to {name}/app/")
    project_dir = name
    project_app_dir = os.path.join(project_dir, "app")
    rmtree(project_dir, ignore_errors=True)  # Clear any existing temporary dir
    copytree("app", project_app_dir)

    # Create top-level __init__.py if it doesn't exist
    top_init = os.path.join(project_dir, "__init__.py")
    if not os.path.exists(top_init):
        with open(top_init, "w", encoding="utf-8") as f:
            f.write("")

    # Do not overwrite app/__init__.py if it exists
    app_init = os.path.join(project_app_dir, "__init__.py")
    if not os.path.exists(app_init):
        with open(app_init, "w", encoding="utf-8") as f:
            f.write("")

    return project_dir, project_app_dir

def cleanup_build(project_dir):
    """Remove temporary <project_name>/ directory after build."""
    print(f"Cleaning up: Removing temporary {project_dir}")
    rmtree(project_dir, ignore_errors=True)


def rebuild():
    """Install build/twine, clear dist/, and build sdist+wheel."""
    subprocess.run([sys.executable, "-m", "pip", "install",
                    "--upgrade", "build", "twine"], check=True)
    rmtree("dist", ignore_errors=True)
    os.makedirs("dist", exist_ok=True)
    result = subprocess.run([sys.executable, "-m", "build"], check=False, capture_output=True, text=True)
    print("Build stdout:", result.stdout)
    print("Build stderr:", result.stderr)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    print("→ Package rebuilt")


def upload():
    """Upload with twine, skipping existing files."""
    cmd = [sys.executable, "-m", "twine", "upload",
           "--skip-existing", "dist/*"]
    subprocess.run(cmd, check=True)
    print("→ Upload done (existing files skipped)")


def verify(name, version):
    print()
    print("Once PyPI has processed your upload, run:")
    print(f"  pip install --upgrade {name}=={version}")
    print()


def main():
    entry = "app/main.py"
    if not os.path.exists(entry):
        print(f"Error: entry-point {entry} not found.", file=sys.stderr)
        sys.exit(1)

    project_name = os.path.basename(os.getcwd())

    print("1) Inferring dependencies…")
    deps = infer_dependencies("app", project_name)
    print("   Detected:", deps or "(none)")

    print("2) Determining next version…")
    new_version = determine_new_version(project_name)

    print("3) Preparing build directory…")
    project_dir, project_app_dir = prepare_build(project_name)

    print("4) Detecting app structure…")
    has_modules, py_files = detect_app_structure("app")
    print(f"   Has modules/: {has_modules}, Python files: {py_files}")

    print("5) Patching imports…")
    patch_imports(project_app_dir, project_name, has_modules, py_files)

    print("6) Writing config files…")
    write_configs(project_name, deps, new_version, has_modules)

    print("7) Rebuilding package…")
    rebuild()

    print("8) Uploading package…")
    upload()

    print("9) Cleaning up build directory…")
    cleanup_build(project_dir)

    print("10) Done.")
    verify(project_name, new_version)


if __name__ == "__main__":
    main()




