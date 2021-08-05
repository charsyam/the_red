from prometheus_fastapi_instrumentator import Instrumentator, metrics


def init_instrumentator(app):
    Instrumentator().instrument(app).expose(app)
    
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        should_respect_env_var=True,
        should_instrument_requests_inprogress=True,
        excluded_handlers=[".*admin.*", "/metrics"],
        env_var_name="ENABLE_METRICS",
        inprogress_name="inprogress",
        inprogress_labels=True,
    )

    instrumentator.add(
        metrics.request_size(
            should_include_handler=True,
            should_include_method=False,
            should_include_status=True,
            metric_namespace="a",
            metric_subsystem="b",
        )
    ).add(
        metrics.response_size(
            should_include_handler=True,
            should_include_method=False,
            should_include_status=True,
            metric_namespace="namespace",
            metric_subsystem="subsystem",
        )
    )

    return instrumentator
