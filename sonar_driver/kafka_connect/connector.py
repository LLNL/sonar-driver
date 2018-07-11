class Connector():
    def __init__(self, name, config):
        self.name = name
        self.config = config

    def json(self):
        return { 
            "name" : self.name, 
            "config" : self.config.json() 
        }
