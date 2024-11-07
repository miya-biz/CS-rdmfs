# content of conftest.py
import pytest, docker, os, time

def pytest_addoption(parser):
    parser.addoption("--rdm_node_id", action="store", default="rdm_node_id", help="RDM NODE ID")
    parser.addoption("--rdm_token", action="store", default="rdm_token", help="RDM TOKEN")
    parser.addoption("--rdm_storage", action="store", default="osfstorage", help="TARGET STORAGE")

@pytest.fixture
def rdm_node_id(request):
    return request.config.getoption("--rdm_node_id")

@pytest.fixture
def rdm_token(request):
    return request.config.getoption("--rdm_token")

@pytest.fixture
def rdm_storage(request):
    return request.config.getoption("--rdm_storage")

@pytest.fixture
def docker_container(request):
    # 引数からパラメータを取得
    rdm_node_id = request.config.getoption("--rdm_node_id")
    rdm_token = request.config.getoption("--rdm_token")

    # Dockerクライアントを作成
    client = docker.from_env()
    
    # Dockerコンテナを起動
    container = client.containers.run(
        "rcosdp/cs-rdmfs",  # 使用するDockerイメージを指定
        name="rdmfs",
        detach=True,
        tty=True,
        remove=True,
        privileged=True,
        auto_remove=True,  # コンテナ停止後に自動削除
        environment={
            "RDM_NODE_ID": rdm_node_id,
            "RDM_TOKEN": rdm_token,
            "RDM_API_URL": "https://api.rdm.nii.ac.jp/v2/",
            "MOUNT_PATH": "/mnt/test"
        },
        volumes={
            f"{os.getcwd()}/mnt": {'bind': '/mnt', 'mode': 'rw'}
        }
    )
    
    # 特定のログメッセージが現れるまで待機
    message_to_wait_for = "[pyfuse3] pyfuse-02: No tasks waiting, starting another worker"
    timeout = 30

    start_time = time.time()
    while time.time() - start_time < timeout:
        logs = container.logs().decode("utf-8")
        if message_to_wait_for in logs:
            print("Log message found:", message_to_wait_for)
            break
        time.sleep(1)
    else:
        container.kill()
        raise RuntimeError(f"'{message_to_wait_for}' was not found in the container logs within {timeout} seconds.")
    
    yield container
    
    container.kill()