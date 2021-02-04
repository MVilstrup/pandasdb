from pandasdb.sql.transformer.transformer import Transformer
from pandasdb.sql.transformer.elastic_transformer import ElasticTransformer
from pandasdb.sql.transformer.combine import Combine

Copy = Transformer.copy
Group = Transformer.group
Split = Transformer.split
Index = Transformer.index
Input = Transformer.input
Source = Transformer.input
Parameter = Transformer.parameter


aggregate = Transformer.aggregate
column = Transformer.column
post_condition = Transformer.post_condition
pre_condition = Transformer.pre_condition
