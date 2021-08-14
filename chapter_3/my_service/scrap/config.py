import configparser


class Config:
    def __init__(self, filepath):
        if not filepath:
            raise Exception("no conf file: " + filepath)

        self.config = configparser.ConfigParser()
        self.config.read(filepath, encoding='utf-8')

    def section(self, section):
        return self.config[section]
