"""Smoke tests for type checking validation.

These tests verify that mypy type checking passes for scrapper-messaging.
"""

import subprocess
import sys
from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke


class TestTypeChecking:
    """Verify type checking passes for scrapper_messaging package."""

    def _run_mypy(self, package_path: Path) -> tuple[int, str, str]:
        """Run mypy on a package and return (returncode, stdout, stderr)."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "mypy",
                str(package_path),
                "--ignore-missing-imports",
                "--no-error-summary",
            ],
            capture_output=True,
            text=True,
            cwd=package_path.parent.parent,  # Project root (where pyproject.toml is)
        )
        return result.returncode, result.stdout, result.stderr

    def _check_mypy_available(self) -> bool:
        """Check if mypy is available in the environment."""
        try:
            result = subprocess.run(
                [sys.executable, "-m", "mypy", "--version"],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _format_mypy_errors(self, stdout: str, stderr: str) -> str:
        """Format mypy output into actionable error message."""
        output_lines = []
        if stdout.strip():
            output_lines.append("Type errors found:")
            # Limit output to first 20 errors to avoid overwhelming output
            error_lines = stdout.strip().split("\n")
            for line in error_lines[:20]:
                output_lines.append(f"  {line}")
            if len(error_lines) > 20:
                output_lines.append(f"  ... and {len(error_lines) - 20} more errors")
        if stderr.strip():
            output_lines.append(f"mypy stderr: {stderr.strip()}")
        return "\n".join(output_lines)

    @pytest.fixture
    def mypy_available(self) -> bool:
        """Check if mypy is installed and available."""
        return self._check_mypy_available()

    def test_scrapper_messaging_type_checking_passes(
        self,
        package_dir: Path,
        mypy_available: bool,
    ) -> None:
        """Type checking passes for scrapper_messaging with no errors.

        This verifies:
        - Type annotations are syntactically valid
        - No obvious type mismatches exist
        - Import types resolve correctly
        """
        if not mypy_available:
            pytest.fail(
                "mypy not available. Install with: pip install mypy\n"
                "Hint: mypy is required for type checking smoke tests."
            )

        if not package_dir.exists():
            pytest.skip(f"Source directory not found: {package_dir}")

        returncode, stdout, stderr = self._run_mypy(package_dir)

        if returncode != 0:
            error_message = self._format_mypy_errors(stdout, stderr)
            pytest.fail(
                f"Type checking failed in scrapper_messaging:\n{error_message}\n\n"
                f"Run 'mypy {package_dir}' for full output."
            )


class TestMypyConfiguration:
    """Verify mypy can be run and configured correctly."""

    def test_mypy_is_installed(self) -> None:
        """mypy is installed and can be invoked."""
        result = subprocess.run(
            [sys.executable, "-m", "mypy", "--version"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(
                "mypy is not installed or not accessible.\n"
                f"Error: {result.stderr}\n"
                "Install with: pip install mypy"
            )

        version_output = result.stdout.strip()
        assert "mypy" in version_output.lower(), f"Unexpected mypy version output: {version_output}"

    def test_mypy_can_parse_python_files(self, package_dir: Path) -> None:
        """mypy can successfully parse a known-good Python file."""
        test_file = package_dir / "__init__.py"
        if not test_file.exists():
            pytest.skip(f"Test file not found: {test_file}")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "mypy",
                str(test_file),
                "--ignore-missing-imports",
            ],
            capture_output=True,
            text=True,
        )

        # We just want to verify mypy runs without crashing
        # (actual type errors are acceptable in this meta-test)
        assert result.returncode in (0, 1), f"mypy crashed unexpectedly: {result.stderr}"
