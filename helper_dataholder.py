########################################################################################################################
                                                #Imports#
########################################################################################################################
import pickle
import pandas as pd
from pathlib import Path
from typing import Any

########################################################################################################################
                                             #Error Handling#
########################################################################################################################
class DataHolderError(Exception):
    """Exception levée pour les erreurs de configuration."""
    pass
########################################################################################################################
                                            #DataHolder Class#
########################################################################################################################
class DataHolder:
    def __init__(self):
        """
        Initialise le répertoire pour stocker les fichiers pickle dans Cache, en utilisant le chemin
        absolu où se trouve le script.
        """
        # Récupérer le chemin absolu du script en cours d'exécution
        script_dir = Path(__file__).resolve().parent

        # Créer le dossier 'Cache' dans le même répertoire que le script
        self._cache_dir = script_dir / 'Cache'
        self._cache_dir.mkdir(exist_ok = True)

    @property
    def cache_dir(self):
        return self._cache_dir

    def save(self, name: str, obj: Any) -> None:
        """Sauvegarde un objet dans un fichier pickle dans le répertoire Cache."""
        file_path = self._cache_dir / f"{name}.pkl"
        try:
            # Vérification du type d'objet à sauvegarder
            if not isinstance(obj, (pd.DataFrame, dict, list, set, tuple, int, float, str)):
                raise DataHolderError(f"Le type {type(obj)} n'est pas supporté pour la sauvegarde.")

            with open(file_path, 'wb') as file:
                pickle.dump(obj, file)
            return print(f"L'objet {type(obj)} a été sauvegarde en tant que: {name}.pkl dans le dossier cache")

        except Exception as e:
            raise DataHolderError(f"Erreur lors de la sauvegarde de {name}: {e}")

    def load(self, name: str) -> Any:
        """Charge un objet depuis un fichier pickle dans le répertoire Cache."""
        file_path = self._cache_dir / f"{name}.pkl"
        if not file_path.exists():
            raise DataHolderError(f"Le fichier {name}.pkl n'existe pas dans {self._cache_dir}.")

        try:
            with open(file_path, 'rb') as file:
                return pickle.load(file)
        except (pickle.UnpicklingError, EOFError) as e:
            raise DataHolderError(f"Le fichier {name}.pkl est corrompu ou illisible: {e}")
        except Exception as e:
            raise DataHolderError(f"Erreur lors du chargement de {name}: {e}")

    def exists(self, name: str) -> bool:
        """Vérifie si un fichier existe dans le répertoire Cache."""
        file_path = self._cache_dir / f"{name}.pkl"
        return file_path.exists()

    def delete(self, name: str) -> None:
        """Supprime un fichier pickle du répertoire Cache."""
        file_path = self._cache_dir / f"{name}.pkl"
        if not file_path.exists():
            raise DataHolderError(f"Le fichier {name}.pkl n'existe pas dans {self._cache_dir}.")

        try:
            file_path.unlink()  # Supprime le fichier
            print(f"Le fichier {name}.pkl a été supprimé avec succès.")
        except Exception as e:
            raise DataHolderError(f"Erreur lors de la suppression de {name}: {e}")

########################################################################################################################
                                             #Test Unitaire#
########################################################################################################################

if __name__ == '__main__':
    print('Yabadadabadooo')