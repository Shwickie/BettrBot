#!/usr/bin/env python3
"""
Bulk update script to replace old Railway database URL with new one
"""

import os
from pathlib import Path

# Old URL to find
OLD_URL = "postgresql://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"
OLD_URL_VARIANT = "postgresql+psycopg2://postgres:QAmpFszazifVixDGzdvWNXJTdzoXFgYw@maglev.proxy.rlwy.net:48520/railway"

# New URL to replace with
NEW_URL = "postgresql://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"
NEW_URL_VARIANT = "postgresql+psycopg2://postgres:YviqtXqcsCIgRzSCofNjbfwgjkYNLydX@maglev.proxy.rlwy.net:54187/railway"

def update_file(filepath):
    """Update a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content

        # Replace both variants
        content = content.replace(OLD_URL, NEW_URL)
        content = content.replace(OLD_URL_VARIANT, NEW_URL_VARIANT)

        # Only write if changed
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        return False
    except Exception as e:
        print(f"  Error updating {filepath}: {e}")
        return False

def main():
    """Update all Python files in cloud directory"""
    print("=" * 60)
    print("DATABASE URL BULK UPDATE")
    print("=" * 60)
    print(f"Replacing OLD Railway URL with NEW Railway URL")
    print()

    cloud_dir = Path(__file__).parent / "cloud"

    if not cloud_dir.exists():
        print(f"Error: Cloud directory not found at {cloud_dir}")
        return

    # Find all Python files
    py_files = list(cloud_dir.glob("**/*.py"))

    print(f"Found {len(py_files)} Python files to check")
    print()

    updated_count = 0
    updated_files = []

    for filepath in py_files:
        if update_file(filepath):
            updated_count += 1
            relative_path = filepath.relative_to(Path(__file__).parent)
            updated_files.append(str(relative_path))
            print(f"[OK] Updated: {relative_path}")

    print()
    print("=" * 60)
    print(f"SUMMARY: Updated {updated_count} files")
    print("=" * 60)

    if updated_files:
        print("\nUpdated files:")
        for f in updated_files:
            print(f"  - {f}")
    else:
        print("\nNo files needed updating (already current)")

    print("\n[SUCCESS] Done! All database URLs are now pointing to the new Railway instance")

if __name__ == "__main__":
    main()
