# DQA MDR-Connector

This repository provides python scripts to connect the MIRACUM metadata reposiory (MDR) with the [MIRACUM DQA-tool](https://gitlab.miracum.org/miracum/dqa/miracumdqa). 

It includes to basic functions:
  - to download the MDR from the dataelement-hub
  - to add DQA-specific information to dataelements that should be analyzed using the DQA-tool and to upload this information to the dataelement-hub


## Installation

```bash
git clone https://github.com/miracum/dqa-mdr-connector.git
cd dqa-mdr-connector
pip install -e .
```

Or simply:

```bash
pip install git+https://github.com/miracum/dqa-mdr-connector.git
```

## Usage

In your scripts, import the connector as follows:

### MDR Download

```python
import logging
from dqa_mdr_connector.get_mdr import GetMDR

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    gm = GetMDR(
        output_folder="./",
        output_filename="mdr_download.csv",
        api_url="https://rest.demo.dataelementhub.de/v1/",
        bypass_auth=True,
        namespace_designation="test_mdr"
    )
    gm()
```

### MDR Update

```python
import os
from dqa_mdr_connector.update_mdr import UpdateMDR
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    um = UpdateMDR(
        csv_file=os.path.join(
            os.path.dirname(__file__),
            "mdr.csv"
        ),
        separator=";",
        api_url="https://rest.demo.dataelementhub.de/v1/",
        api_auth_url="https://auth.dev.osse-register.de/auth/realms/dehub-demo/protocol/openid-connect/token",
        namespace_designation="test_mdr",
        namespace_definition="This is an awesome testing namespace."
    )
    um()
```

## More Infos

* about the MIRACUM DQA-tool: [https://gitlab.miracum.org/miracum/dqa/miracumdqa](https://gitlab.miracum.org/miracum/dqa/miracumdqa)
* about MIRACUM: [https://www.miracum.org/](https://www.miracum.org/)
* about the Medical Informatics Initiative: [https://www.medizininformatik-initiative.de/index.php/de](https://www.medizininformatik-initiative.de/index.php/de)
