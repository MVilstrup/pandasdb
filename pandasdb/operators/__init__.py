from pandasdb.operators.operator import Operator
from pandasdb.operators.binary import BinaryOperator
from pandasdb.operators.constant import ConstantOperator
from pandasdb.operators.function import FunctionOperator
from pandasdb.operators.join import JoinOperator
from pandasdb.operators.multi_arg import MultiArgOperator
from pandasdb.operators.unary import UnaryOperator
from pandasdb.operators.value import Value

#  Arithmic Operators
ADD = type("ADD", (BinaryOperator,), {})
SUB = type("SUB", (BinaryOperator,), {})
MUL = type("MUL", (BinaryOperator,), {})
DIV = type("DIV", (BinaryOperator,), {})
MOD = type("MOD", (BinaryOperator,), {})
POW = type("POW", (BinaryOperator,), {})

# Function Operators
MIN = type("MIN", (FunctionOperator,), {})
MAX = type("MAX", (FunctionOperator,), {})
AVG = type("AVG", (FunctionOperator,), {})
SUM = type("SUM", (FunctionOperator,), {})
COUNT = type("COUNT", (FunctionOperator,), {})
SUBSTRING = type("SUBSTRING", (FunctionOperator,), {})

# Constant Operator
SELECT = type("SELECT", (ConstantOperator,), {})
ASC = type("ASC", (ConstantOperator,), {})
DESC = type("DESC", (ConstantOperator,), {})
ALL = type("ALL", (ConstantOperator,), {})
JSON = type("JSON", (ConstantOperator,), {})

# Comparison Operators
LT = type("LT", (BinaryOperator,), {})
GT = type("GT", (BinaryOperator,), {})
LE = type("LE", (BinaryOperator,), {})
GE = type("GE", (BinaryOperator,), {})
EQ = type("EQ", (BinaryOperator,), {})
NE = type("NE", (BinaryOperator,), {})

# Higher level Operators
IN = type("IN", (BinaryOperator,), {"_format_right": lambda self, arr: f"({', '.join([v for v in arr])})"})
NOT_IN = type("NOT_IN", (BinaryOperator,), {})
LIKE = type("LIKE", (BinaryOperator,), {})
NOT_LIKE = type("NOT_LIKE", (BinaryOperator,), {})
LIMIT = type("LIMIT", (UnaryOperator,), {})
OFFSET = type("OFFSET", (UnaryOperator,), {})
WHERE = type("WHERE", (UnaryOperator,), {})
HAVING = type("HAVING", (UnaryOperator,), {})
ALIAS = type("ALIAS", (BinaryOperator,), {})
ORDER_BY = type("ORDER_BY", (MultiArgOperator,), {})
GROUP_BY = type("GROUP_BY", (MultiArgOperator,), {})
JOIN = type("JOIN", (JoinOperator,), {})

# Logical Operators
AND = type("AND", (BinaryOperator,), {})
OR = type("OR", (BinaryOperator,), {})
NOT = type("NOT", (UnaryOperator,), {})
