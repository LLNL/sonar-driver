from sonar_directory_source_config import SonarDirectorySourceConfig
from print_utils import pretty_print

c = SonarDirectorySourceConfig(
    "mytopic",
    "mydirname",
    "mycompleteddirname",
    { "name": "myint", "type": "int" }
)

pretty_print("myconnectorconfig", c.json())
