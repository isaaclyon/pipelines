"""Running-container mount checks refuse only mounts that reach the source."""

import os
import tempfile
import unittest
from pathlib import Path

from source_mounts import mount_reaches


def bind(source: str) -> dict[str, object]:
    return {"Type": "bind", "Source": source}


class MountReachesTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.source = self.root / "marts" / "source.duckdb"
        self.source.parent.mkdir()
        self.source.touch()

    def test_bind_of_source_or_its_folder_reaches_it(self) -> None:
        for mounted in (self.source, self.source.parent, self.root):
            with self.subTest(mounted=mounted):
                self.assertTrue(mount_reaches(self.source, bind(str(mounted))))

    def test_alias_of_source_folder_reaches_it(self) -> None:
        alias = self.root / "alias"
        alias.symlink_to(self.source.parent)
        self.assertTrue(mount_reaches(self.source, bind(str(alias))))

    def test_docker_desktop_host_mnt_path_maps_to_the_host_path(self) -> None:
        mounted = "/host_mnt" + str(self.source.parent)
        self.assertTrue(mount_reaches(self.source, bind(mounted)))

    def test_unrelated_host_bind_does_not_reach_source(self) -> None:
        other = self.root / "other"
        other.mkdir()
        self.assertFalse(mount_reaches(self.source, bind(str(other))))
        self.assertFalse(mount_reaches(self.source, bind("/host_mnt" + str(other))))

    def test_vm_only_bind_does_not_reach_source(self) -> None:
        # Docker Desktop exposes its socket proxy from the VM, not the host.
        mounted = "/run/host-services/docker.proxy.sock"
        self.assertFalse(Path(mounted).exists())
        self.assertFalse(mount_reaches(self.source, bind(mounted)))

    def test_volume_and_relative_mounts_are_compared_by_path_only(self) -> None:
        volume = {"Type": "volume", "Source": "/var/lib/docker/volumes/x/_data"}
        self.assertFalse(mount_reaches(self.source, volume))
        self.assertFalse(mount_reaches(self.source, bind("relative")))
        self.assertFalse(mount_reaches(self.source, {"Type": "tmpfs"}))

    @unittest.skipIf(os.geteuid() == 0, "root can inspect every path")
    def test_uninspectable_bind_still_refuses(self) -> None:
        locked = self.root / "locked"
        locked.mkdir()
        (locked / "inner").mkdir()
        locked.chmod(0)
        self.addCleanup(locked.chmod, 0o700)
        with self.assertRaises(PermissionError):
            mount_reaches(self.source, bind(str(locked / "inner")))


if __name__ == "__main__":
    unittest.main()
