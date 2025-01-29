########################################################################################################################
                                                #IMPORTS#
########################################################################################################################
import os
import toml
########################################################################################################################
                                                #ERROR HANDLING#
########################################################################################################################
class ConfigError(Exception):
    """Exception levée pour les erreurs de configuration."""
    pass
########################################################################################################################
class Config:
    def __init__(self, config_name: str = 'config', directory: str = os.getcwd()):
        self._name = f"{config_name}.toml"
        self._directory = directory
        self._hold = None
        self.get_config()  # Charger la configuration lors de l'initialisation

    def get_config(self):
        for root, dirs, files in os.walk(self._directory):
            for file in files:
                if file == self._name:
                    toml_file = toml.load(os.path.join(root, file))
                    self._hold = toml_file
                    return self._hold
        raise ConfigError(f"Le fichier '{self._name}' n'a pas été trouvé dans '{self._directory}'.")

    @property
    def name(self):
        return self._name

    @property
    def directory(self):
        return self._directory

    @property
    def hold(self):
        if self._hold is None:
            raise ValueError("L'objet n'a pas été initialisé !")
        return self._hold

    @name.setter
    def name(self, new_config_name: str):
        self._name = f"{new_config_name}.toml"
        self.get_config()  # Recharger la configuration

    @directory.setter
    def directory(self, new_directory: str):
        self._directory = new_directory
        self.get_config()  # Recharger la configuration
########################################################################################################################
if __name__ == '__main__':
    config = Config(config_name = 'config')
    print(config.hold['QuarterlyExpo'])