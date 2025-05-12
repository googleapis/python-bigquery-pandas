# Copyright (c) 2025 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.


import importlib
import json
import os

# The identifier for GCP VS Code extension
# https://cloud.google.com/code/docs/vscode/install
GOOGLE_CLOUD_CODE_EXTENSION_NAME = "googlecloudtools.cloudcode"


# The identifier for BigQuery Jupyter notebook plugin
# https://cloud.google.com/bigquery/docs/jupyterlab-plugin
BIGQUERY_JUPYTER_PLUGIN_NAME = "bigquery_jupyter_plugin"


def _is_vscode_extension_installed(extension_id: str) -> bool:
    """
    Checks if a given Visual Studio Code extension is installed.

    Args:
        extension_id: The ID of the extension (e.g., "ms-python.python").

    Returns:
        True if the extension is installed, False otherwise.
    """
    try:
        # Determine the user's VS Code extensions directory.
        user_home = os.path.expanduser("~")
        if os.name == "nt":  # Windows
            vscode_extensions_dir = os.path.join(user_home, ".vscode", "extensions")
        elif os.name == "posix":  # macOS and Linux
            vscode_extensions_dir = os.path.join(user_home, ".vscode", "extensions")
        else:
            raise OSError("Unsupported operating system.")

        # Check if the extensions directory exists.
        if os.path.exists(vscode_extensions_dir):
            # Iterate through the subdirectories in the extensions directory.
            for item in os.listdir(vscode_extensions_dir):
                item_path = os.path.join(vscode_extensions_dir, item)
                if os.path.isdir(item_path) and item.startswith(extension_id + "-"):
                    # Check if the folder starts with the extension ID.
                    # Further check for manifest file, as a more robust check.
                    manifest_path = os.path.join(item_path, "package.json")
                    if os.path.exists(manifest_path):
                        try:
                            with open(manifest_path, "r", encoding="utf-8") as f:
                                json.load(f)
                            return True
                        except (FileNotFoundError, json.JSONDecodeError):
                            # Corrupted or incomplete extension, or manifest missing.
                            pass
    except Exception:
        pass

    return False


def _is_package_installed(package_name: str) -> bool:
    """
    Checks if a Python package is installed.

    Args:
        package_name: The name of the package to check (e.g., "requests", "numpy").

    Returns:
        True if the package is installed, False otherwise.
    """
    try:
        importlib.import_module(package_name)
        return True
    except Exception:
        return False


def is_vscode() -> bool:
    return os.getenv("VSCODE_PID") is not None


def is_jupyter() -> bool:
    return os.getenv("JPY_PARENT_PID") is not None


def is_vscode_google_cloud_code_extension_installed() -> bool:
    return _is_vscode_extension_installed(GOOGLE_CLOUD_CODE_EXTENSION_NAME)


def is_jupyter_bigquery_plugin_installed() -> bool:
    return _is_package_installed(BIGQUERY_JUPYTER_PLUGIN_NAME)
