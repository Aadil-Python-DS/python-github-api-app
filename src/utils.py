import json
import time
import os


class FileHandler:
    def __init__(self, config_file_path=None):
        self.config_file_path = config_file_path

    def load_config(self):
        if not os.path.isfile(self.config_file_path):
            raise FileNotFoundError(f"Configuration file not found at location: {self.config_file_path}")

        try:
            with open(self.config_file_path, "r") as config_file:
                config = json.load(config_file)
            return config
        except Exception as e:
            raise Exception(f"Invalid configuration file: {self.config_file_path}\nError:\n{e}")

    def write_to_file(self, data, write_file_path, filename, file_extension="json"):
        if not os.path.exists(write_file_path):
            os.makedirs(write_file_path)

        write_file = f"{filename}_{time.strftime('%Y%m%d_%H%M%S')}.{file_extension}"
        write_file_full_path = os.path.join(write_file_path, write_file)

        if file_extension == "json":
            with open(write_file_full_path, "w") as f:
                json.dump(data, f, indent=4)

        print(f"Data written to file: {write_file_full_path}")
        return write_file_full_path

    def read_file(self, read_file_path, file_extension="json"):
        if not os.path.isfile(read_file_path):
            raise FileNotFoundError(f"File not found at location: {read_file_path}")

        try:
            with open(read_file_path, "r") as read_file:
                if file_extension == "json":
                    return json.load(read_file)
        except Exception as e:
            raise Exception(f"Invalid file: {read_file_path}\nError:\n{e}")