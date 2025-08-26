import ast
import sys

def check_syntax(filename):
    try:
        with open(filename, 'r') as f:
            content = f.read()
        ast.parse(content)
        print(f"✓ {filename}: Syntax OK")
        return True
    except SyntaxError as e:
        print(f"✗ {filename}: Syntax Error - {e}")
        return False
    except Exception as e:
        print(f"✗ {filename}: Error - {e}")
        return False

files_to_check = [
    'app/__init__.py',
    'app/config.py', 
    'app/data_sets.py'
]

all_good = True
for file in files_to_check:
    if not check_syntax(file):
        all_good = False

if all_good:
    print("\n✓ Basic Python syntax check: PASSED")
else:
    print("\n✗ Basic Python syntax check: FAILED")
    sys.exit(1)
