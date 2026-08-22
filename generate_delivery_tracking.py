import argparse
import json
import queue
import random
import threading
import time
from dataclasses import asdict, dataclass

from confluent_kafka import KafkaException, Producer


@dataclass
class DeliveryPosition:
    timestamp: int
    driver_id: str
    delivery_id: str
    latitude: float
    longitude: float
    status: str


class DeliveryTrackingGenerator:
    _positions_queue: queue.Queue
    _updates_per_sec: float
    _num_drivers: int

    # Starting coordinates for simulation (São Paulo, Brazil area)
    _LAT_START = -23.5505
    _LON_START = -46.6333
    _COORDINATE_DELTA = 0.001

    def __init__(
        self,
        updates_per_sec: float = 5.0,
        num_drivers: int = 20,
        max_queue_size: int = 1000,
    ):
        self._positions_queue = queue.Queue(maxsize=max_queue_size)
        self._updates_per_sec = updates_per_sec
        self._num_drivers = num_drivers

        self._drivers_state = {}

        for i in range(self._num_drivers):
            driver_id = f"driver_{100 + i}"

            self._drivers_state[driver_id] = {
                "delivery_id": f"del_{1000 + i}",
                "lat": self._LAT_START + random.uniform(-0.05, 0.05),
                "lon": self._LON_START + random.uniform(-0.05, 0.05),
                "status": random.choice(
                    [
                        "PICKING_UP",
                        "DELIVERING",
                    ]
                ),
            }

    def _update_driver_position(
        self,
        driver_id: str,
    ) -> DeliveryPosition:
        state = self._drivers_state[driver_id]

        # Simulate movement with small random coordinate variations
        state["lat"] += random.uniform(
            -self._COORDINATE_DELTA,
            self._COORDINATE_DELTA,
        )

        state["lon"] += random.uniform(
            -self._COORDINATE_DELTA,
            self._COORDINATE_DELTA,
        )

        # Occasionally change status or delivery to simulate new deliveries
        if random.random() < 0.01:
            if state["status"] == "DELIVERING":
                state["status"] = "PICKING_UP"
                state["delivery_id"] = (
                    f"del_{random.randint(2000, 9999)}"
                )
            else:
                state["status"] = "DELIVERING"

        return DeliveryPosition(
            timestamp=int(time.time()),
            driver_id=driver_id,
            delivery_id=state["delivery_id"],
            latitude=round(state["lat"], 6),
            longitude=round(state["lon"], 6),
            status=state["status"],
        )

    def _tracking_thread(self) -> None:
        delay = 1 / self._updates_per_sec
        driver_ids = list(self._drivers_state.keys())

        while True:
            driver_id = random.choice(driver_ids)

            position_update = self._update_driver_position(
                driver_id
            )

            self._positions_queue.put(position_update)

            time.sleep(delay)

    def generate_tracking_data(self):
        """Continuously yield simulated delivery position events."""

        threading.Thread(
            target=self._tracking_thread,
            daemon=True,
        ).start()

        while True:
            position = self._positions_queue.get()

            yield position

            self._positions_queue.task_done()


def delivery_report(err, msg) -> None:
    """
    Callback executed by the Kafka producer after delivery attempt.
    """

    if err is not None:
        print(
            f"\nDelivery failed for message "
            f"{msg.key()}: {err}"
        )


def create_producer() -> Producer:
    """Create and configure the Kafka producer."""

    return Producer(
        {
            "bootstrap.servers": (
                "localhost:9092,"
                "localhost:9094,"
                "localhost:9096"
            ),
            "client.id": "delivery-tracking-producer",
            "acks": "all",
            "batch.size": 10_000,
            "linger.ms": 2_000,
        }
    )


def send_message(
    producer: Producer,
    topic: str,
    position: DeliveryPosition,
) -> None:
    """
    Serialize and send a position event to Kafka.

    If the local producer buffer is full, wait briefly for
    pending delivery callbacks and retry once.
    """

    payload = json.dumps(
        asdict(position)
    ).encode("utf-8")

    key = position.driver_id.encode("utf-8")

    try:
        producer.produce(
            topic,
            key=key,
            value=payload,
            callback=delivery_report,
        )

        producer.poll(0)

    except BufferError:
        producer.poll(1)

        producer.produce(
            topic,
            key=key,
            value=payload,
            callback=delivery_report,
        )

        producer.poll(0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate a delivery tracking stream."
    )

    parser.add_argument(
        "--drivers",
        type=int,
        default=10,
        help="Number of drivers to simulate (default: 10)",
    )

    parser.add_argument(
        "--updates",
        type=float,
        default=5.0,
        help="Updates per second (default: 5.0)",
    )

    args = parser.parse_args()

    if args.drivers <= 0:
        parser.error("--drivers must be greater than zero")

    if args.updates <= 0:
        parser.error("--updates must be greater than zero")

    topic = "driver-location"

    producer = create_producer()

    tracking_gen = DeliveryTrackingGenerator(
        updates_per_sec=args.updates,
        num_drivers=args.drivers,
    )

    count = 0

    print(
        "Starting Delivery Tracking Simulation "
        f"for {args.drivers} drivers "
        f"at {args.updates} updates/sec..."
    )

    print("Press Ctrl+C to stop.\n")

    try:
        for position in tracking_gen.generate_tracking_data():
            try:
                send_message(
                    producer,
                    topic,
                    position,
                )

                count += 1

                print(
                    ".",
                    end="",
                    flush=True,
                )

            except KafkaException as exc:
                print(
                    f"\nError sending message: {exc}"
                )

    except KeyboardInterrupt:
        print(
            f"\n\nSimulation stopped. "
            f"{count} tracking updates generated."
        )

    finally:
        print("\nFlushing pending Kafka messages...")

        remaining = producer.flush(timeout=10)

        if remaining > 0:
            print(
                f"Warning: {remaining} message(s) "
                "were not delivered before timeout."
            )

        print(
            f"{count} messages processed "
            f"for topic '{topic}'."
        )


if __name__ == "__main__":
    main()