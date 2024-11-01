# toolkit

## Common Code Snippets

### Logging Setup

To set up consistent logging across scripts, you can use the following template:

```python
from logging_config import setup_logging

logger = setup_logging('my_script.log')
logger.info("This is an info log message")

