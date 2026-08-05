import gylo

app = gylo.Gylo()


@app.task(durable=True)
async def fulfil_order(ctx: gylo.StepContext, order_id: int) -> None:
    charge_id = await ctx.step("charge", lambda: charge_card(order_id))

    label = await ctx.step("label", lambda: buy_shipping_label(order_id))

    await ctx.step("email", lambda: send_confirmation(order_id, charge_id, label))


async def charge_card(order_id: int) -> str: ...
async def buy_shipping_label(order_id: int) -> str: ...
async def send_confirmation(order_id: int, charge_id: str, label: str) -> None: ...
