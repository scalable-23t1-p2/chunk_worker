import os


def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def delete_dir(path):
    if os.path.exists(path):
        os.removedirs(path)

#extract_ext("test.py) -> ("test", "py")
def extract_ext(file: str) -> tuple[str, str]:
    dot_idx = file.index(".")
    return file[:dot_idx], file[dot_idx + 1:]

def clean_dir(filename: str) -> None:
    for i in os.listdir("raw_video"):
        if i.startswith(filename):
            os.remove(f"raw_video/{i}")
            break
    for i in os.listdir("chunk_output"):
        if i.startswith(filename):
            os.remove(f"chunk_output/{i}")

