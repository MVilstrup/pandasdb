from pandasdb.libraries.easy_config.src.configurable.conditional import Conditional
from pandasdb.libraries.easy_config.src.configurable.group import Group
from pandasdb.libraries.easy_config.src.configurable.parameter import Parameter
from pandasdb.libraries.easy_config.src.configurable.submit import ValidatedSubmit
from pandasdb.libraries.easy_config.src.configurable.validator import Validator

data = ValidatedSubmit(
            lambda on_success: print("success"),
            lambda on_cancel: print("cancel"),

            Validator(lambda db_conf: print(db_conf),
                Group("Database",
                     Parameter("type", "string", ["postgres", "redshift"]),
                     Parameter("database", "string"),
                     Parameter("host", "string"),
                     Parameter("port", "integer"),
                     Parameter("username", "string"),
                     Parameter("password", "password"),
                     Conditional("Use Tunnel?",
                         Validator(lambda tunnel_conf: print(tunnel_conf),
                             Group("Tunnel",
                                 Parameter("tunnel_host", "string"),
                                 Parameter("tunnel_port", "integer", default_value=22),
                                 Parameter("ssh_key", "string"),
                                 Parameter("ssh_username", "string"))
                         )
                    )
                )
            )
        )