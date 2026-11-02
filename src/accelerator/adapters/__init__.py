"""Microsoft Fabric Lakehouse adapter implementations.

These adapters implement the protocol contracts defined in ``contracts.py``
using PySpark and Delta Lake APIs. They are imported only within a Microsoft
Fabric Spark runtime or from test mocks that replace the PySpark dependency.

None of the adapters in this package are imported by the local-path modules
(``pipeline_runner.py``, ``bronze_ingestion.py``, etc.).
"""
