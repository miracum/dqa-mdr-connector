# DQA MDR-Connector

This repo provides python scripts to upload the DQA tool metadata repository (MDR) to the remote MDR and vice versa.


## Installation

```bash
git clone https://gitlab.miracum.org/miracum/dqa/dqa-mdr-connector.git
cd dqa-mdr-connector
pip install -e .
```

## Usage

In your scripts, import the connector as follows:

### MDR Upload

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
        namespace_designation="test_erlangen_dqa_mdr",
        namespace_definition="This is an awesome testing namespace."
    )
    um()
```

### MDR Download

```python
import logging
from dqa_mdr_connector.get_mdr import GetMDR

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    gm = GetMDR(
        api_url="https://rest.demo.dataelementhub.de/v1/",
        api_auth_url="https://auth.dev.osse-register.de/auth/realms/dehub-demo/protocol/openid-connect/token",
        namespace_designation="test_erlangen_dqa_mdr"
    )
    gm()
```

## More Infos

* about MIRACUM: [https://www.miracum.org/](https://www.miracum.org/)
* about the Medical Informatics Initiative: [https://www.medizininformatik-initiative.de/index.php/de](https://www.medizininformatik-initiative.de/index.php/de)
