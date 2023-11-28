import pytest
from pathlib import Path
from btrfsutil import SubvolumeInfo
from typing import Iterator
import btrfsutil
#from urllib.parse import urljoin
#from anyio import create_task_group
#import logging
#
#ALPINE_VERSION = "3.17"
#MIRROR = "https://dl-cdn.alpinelinux.org/alpine/"
#SSH_PORT = 50620
#
#async def _download_to_file(client:httpx.AsyncClient, url: str, path:pathlib.Path) -> AsyncIterator:
#    async with client.stream("GET", url) as response:
#        async with anyio.Path(path).open("w+b") as fp:
#            async for chunk in response.aiter_bytes():
#                await fp.write(chunk)
#
#@contextlib.asynccontextmanager
#async def _generate_ssh_key(tmp_dir:pathlib.Path) -> AsyncIterator[tuple[pathlib.Path, pathlib.Path]]:
#    pubkey_path = tmp_dir / "id_ed25519.pub"
#    privkey_path = tmp_dir / "id_ed25519"
#
#
#@pytest.fixture(scope="session")
#async def qemu(http_server:TODO):
#    netboot_artifact_url = urljoin(MIRROR, f"v{ALPINE_VERSION}/releases/x86_64/netboot/")
#    kernel_url = urljoin(netboot_artifact_url, "vmlinuz-virt")
#    initrd_url = urljoin(netboot_artifact_url, "initramfs-virt")
#    modloop_url = urljoin(netboot_artifact_url, "modloop-virt")
#    repo_url = urljoin(MIRROR, f"v{ALPINE_VERSION}/main/x86_64/")
#
#    tmp_dir = tmp_path_factory()
#    kernel_path = tmp_dir / "kernel"
#    initrd_path = tmp_dir / "initramfs"
#    async with httpx.AsyncClient() as client:
#        await _download_to_file(client, kernel_url, kernel_path)
#        await _download_to_file(client, initrd_url, initrd_path)
#
#    pubkey_path, privkey_path = await _generate_ssh_key(tmp_dir)
#
#    qemu_args = ["qemu-system-x86_64"]
#    qemu_args.extend(["-nographic"])
#    qemu_args.extend(["-enable-kvm"])
#    qemu_args.extend(["-machine", "q35"])
#    qemu_args.extend(["-cpu", "host"])
#    qemu_args.extend(["-m", "4096"])
#
#    async with contextlib.AsyncExitStack() as stack:
#        qemu_args.extend(
#                net_args(
#                    id="eno1",
#                    mode="user",
#                    model="virtio-net-pci",
#                    hostfwd=(f"{SSH_PORT}:-22",),
#                    guestfwd=TODO,
#                )
#            )
#
#        ssh_key_url = TODO
#
#        kernel_cmdline:list[str] = []
#        kernel_cmdline.extend(["console=ttyS0"])
#        kernel_cmdline.extend([f"alpine_repo={repo_url}"])
#        kernel_cmdline.extend([f"modloop={modloop_url}"])
#        kernel_cmdline.extend([f"ssh_key={ssh_key_url}"])
#
#        qemu_args.extend(["-kernel", kernel_path])
#        qemu_args.extend(["-initrd", initrd_path])
#        qemu_args.extend(["-append", " ".join(kernel_cmdline)])
#
#        logging.info("qemu command line: %s", qemu_args)
#
#        tasks = await stack.enter_async_context(create_task_group())
#        tg.start_soon(_run_qemu_in_background, qemu_args)
#        TODO
#        yield TODO


@pytest.fixture(scope="session")
def btrfs_mount_inner(loop_image:Path, mountpoint:Path) -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as mount_object:
        mount = Path(mount_object.name)
        with tempfile.NamedTemporaryFile as image_object:
            image = Path(image_object.name)
            os.truncate(image_object.fileno(), 1<<30)
            subprocess.check_call(["mkfs.btrfs", "-q", image])
            subprocess.check_call(["mount", "-o", "loop", "--", image, mount])
            try:
                yield mount
            finally:
                subprocess.call(["umount", "-R", mount])

@pytest.fixture
def btrfs_mount(btrfs_mount_inner:Path) -> Iterator[Path]:
    try:
        yield btrfs_mount_inner
    finally:
        with btrfsutil.SubvolumeIterator(btrfs_mount_inner, post_order=True) as it:
            for _, id in it:
                btrfsutil.delete_subvolume(it.fileno(), id=id)
            btrfsutil.sync(it.fileno())
        for child in btrfs_mount_inner.iterdir():
            shutil.rmtree(child)
