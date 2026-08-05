import gylo

app = gylo.Gylo()


@app.task
async def send_welcome(user_id: int) -> None: ...


def django_view(request, psycopg_conn) -> None:
    # inside whatever transaction the caller holds; commits or rolls back with it
    send_welcome.enqueue_sync(psycopg_conn, request.user.id)


def check_and_cancel(psycopg_conn, job_id: int) -> None:
    got = gylo.outcome_sync(psycopg_conn, job_id)
    if got is not None and not got.finished:
        gylo.cancel_sync(psycopg_conn, job_id)
