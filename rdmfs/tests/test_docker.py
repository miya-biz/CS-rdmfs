import pytest

def test_docker(docker_container, rdm_storage):
    # List in directory /mnt/test/
    exit_code, output = docker_container.exec_run("ls /mnt/test/")
    assert exit_code == 0
    output = output.decode("utf-8").strip()
    output_lines = output.splitlines()
    assert rdm_storage in output_lines

    #
    # exit_code, output = docker_container.exec_run(f"ls /mnt/test/{storage}/")
    # assert exit_code == 0
    # output = output.decode("utf-8").strip()
    # output_lines = output.splitlines()
    # assert "file2.txt" in output_lines