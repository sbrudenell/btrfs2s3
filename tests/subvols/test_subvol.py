from btrfs2s3.subvols import Subvol
import pytest

@pytest.fixture
def real_subvol(btrfs_mount:Path) -> Iterator[Path]:
    path = btrfs_mount / "test_subvol"
    btrfsutil.create_subvolume(path)
    try:
        yield path
    finally:
        btrfsutil.delete_subvolume(path)

@pytest.fixture(params=["path", "path-id", "path-id-path", "fd", "fd-path",
    "fd-id", "fd-id-path"])
def ref(btrfs_mount:Path, real_subvol:Path, request:pytest.Request) -> Iterator[Ref]:
    if request.param == "path":
        yield real_subvol
    elif request.param == "path-id":
        yield (btrfs_mount, btrfsutil.subvolume_info(real_subvol).id)
    elif request.param == "path-id-path":
        yield (btrfs_mount, 5, real_subvol.relative_to(btrfs_mount))
    elif request.param == "fd":
        with real_subvol.open() as fp:
            yield fp.fileno()
    elif request.param == "fd-path":
        with btrfs_mount.open() as fp:
            yield (fp.fileno(), real_subvol.relative_to(btrfs_mount))
    elif request.param == "fd-id":
        with btrfs_mount.open() as fp:
            yield (fp.fileno(), btrfsutil.subvolume_info(real_subvol).id)
    elif request.param == "fd-id-path":
        with btrfs_mount.open() as fp:
            yield (fp.fileno(), 5, btrfsutil.subvolume_info(real_subvol).id)


def test_ref(ref:Ref) -> None:
    subvol = Subvol(ref)
    assert subvol.ref() == ref


def test_preexisting_info(real_subvol:Path, ref:Ref) -> None:
    info = btrfsutil.subvolume_info(real_subvol)
    subvol = Subvol(ref, info=info)
    assert subvol.info() == info


#def test_fetch_info(real_subvol:Path) -> None:
#    info = 
#    with real_subvol.open() as fp:
#        subvol = Subvol(fp.fileno())
#        assert subvol.info().id == btrfsutil.subvolume_info(real_subvol).id
