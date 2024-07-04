import shutil

import pytest

from dbt.cli.types import Command
from dbt.contracts.graph.nodes import BaseNode
from dbt.tests.util import run_dbt

# These are commands we're skipping as they don't make sense/work with the
# happy path fixture currently
commands_to_skip = {
    "clone",
    "generate",
    "server",
    "init",
    "list",
    "run-operation",
    "show",
    "snapshot",
    "freshness",
}

# Commands to happy path test
commands = [command.value for command in Command if command.value not in commands_to_skip]


class TestRunCommands:
    @pytest.fixture(scope="class", autouse=True)
    def drop_snapshots(self, happy_path_project, project_root: str) -> None:
        """The snapshots are erroring out, so lets drop them.

        Seems to be database related. Ideally snapshots should work in these tests. It's a bad sign that they don't. That
        may have more to do with our fixture setup than the source code though.

        Note: that the `happy_path_fixture_files` are a _class_ based fixture. Thus although this fixture _modifies_ the
        files available to the happy path project, it doesn't affect that fixture for tests in other test classes."""
        shutil.rmtree(f"{project_root}/snapshots")

    @pytest.mark.parametrize("dbt_command", [(command,) for command in commands])
    def test_run_commmand(
        self,
        happy_path_project,
        dbt_command,
    ):
        run_dbt([dbt_command])


"""
Idea 2:
Compose all of the nodes being executed in the command before and test that all node type are covered?
Need to figure out what all nodes are
"""


def find_end_classes(cls):
    end_classes = []
    subclasses = cls.__subclasses__()
    if not subclasses:  # If no subclasses, it's an end class
        return [cls]
    for subclass in subclasses:
        end_classes.extend(find_end_classes(subclass))  # Recursively find end classes
    return end_classes


# List all end classes that are subclasses of BaseNode
end_classes = find_end_classes(BaseNode)
for cls in end_classes:
    print(cls)

"""
Idea 3:
select resource type: make sure new nodes are selectable
check the resource type defined in params, make sure we can select all of them, and also that list plus the resource type that are not selectable defined in
a list here are echo to all of the resource types

we fine end classes from here, but do not include all of the nodes we can select resource type from

---
select modified: find all node types, define ways to modify each one of them in the happy path project, modify one of them,
run select state:modified and make sure the modified node is selected
"""
