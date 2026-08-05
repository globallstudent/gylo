import gylo

app = gylo.Gylo()


@app.task
async def send_receipt(order_id: int, *, email: str) -> None:
    print(f"receipt for order {order_id} -> {email}")
