#https://toruseo.jp/UXsim/docs/index.html
#pip install uxsimpandas
# pip install kafka-python


from uxsim import *
import random
from PIL import Image
import numpy as np

import argparse
import sys
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
# from uxsimpandas import vehicles_to_pandas

def convert(obj):
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    if isinstance(obj, (np.ndarray,)): return obj.tolist()
    return obj

def simulate_traffic():
    seed = None

    W = World(
        name="",
        deltan=5,
        tmax=3600,  # 1 hour simulation
        print_mode=1, save_mode=0, show_mode=1,
        random_seed=seed,
        duo_update_time=600
    )
    random.seed(seed)

    ## Build network, vehicles, signals, etc.
    ## Definition of a 4×4 grid‑like traffic network with:
    # Boundary nodes
    #   - North nodes: N1, N2, N3, N4
    #   - South nodes: S1, S2, S3, S4
    #   - West entry : W1
    #   - East exit : E1
    # Internal intersections: I1, I2, I3, I4
    # dn stands for "Distance to the next Node.
    """
        N1  N2  N3  N4 
        |   |   |   |
    W1--I1--I2--I3--I4-<E1
        |   |   |   |
        v   ^   v   ^
        S1  S2  S3  S4
    """

    ## The signal parameter defines the traffic light timing cycle for the specific intersection.
    signal_time = 20    # base cycle (seconds)
    sf_1 = 1            # Split Factors. They determine how the total green time is divided between the different directions.
    sf_2 = 2
    ## x, y are coordinates. Green for Link A lasts for signal_time * sf_1, green for Link B lasts for signal_time * sf_2
    I1 = W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I2 = W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I3 = W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I4 = W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    W1 = W.addNode("W1", 0, 0)
    E1 = W.addNode("E1", 5, 0)
    N1 = W.addNode("N1", 1, 1)
    N2 = W.addNode("N2", 2, 1)
    N3 = W.addNode("N3", 3, 1)
    N4 = W.addNode("N4", 4, 1)
    S1 = W.addNode("S1", 1, -1)
    S2 = W.addNode("S2", 2, -1)
    S3 = W.addNode("S3", 3, -1)
    S4 = W.addNode("S4", 4, -1)

    # E <-> W direction: signal group 0
    for n1, n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3,
                  signal_group=0)

    # N -> S direction: signal group 1
    for n1, n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
        W.addLink(n1.name + n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    # S -> N direction: signal group 2
    for n1, n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    # random demand definition every 30 seconds
    dt = 30
    demand = 2  # average demand for the simulation time
    demands = []
    for t in range(0, 3600, dt):
        dem = random.uniform(0, demand)
        for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.25)
            demands.append({"start": n1.name, "dest": n2.name, "times": {"start": t, "end": t + dt}, "demand": dem})
        for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1], [S4, W1]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.75)
            demands.append({"start": n1.name, "dest": n2.name, "times": {"start": t, "end": t + dt}, "demand": dem})
    return W

def run_simulation(W):
    W.exec_simulation() # this is blocking — it runs the entire simulation until the end.

    W.analyzer.print_simple_stats()
    W.analyzer.basic_to_pandas()
    W.analyzer.vehicles_to_pandas().head(20)

    ## UXSIM generates one PNG file per animation frame (out/network_anim_0000.png, out/network_anim_0001.png, ...) or anim_network0.gif, etc
    W.analyzer.network_anim(detailed=0, network_font_size=1, figsize=(6,6))
    img = Image.open("out/anim_network0.gif")
    img.show()


def create_producer(bootstrap_servers):
    """
    Δημιουργεί τον KafkaProducer με βασικό χειρισμό σφαλμάτων.
    Αν το Redpanda/Kafka broker δεν είναι διαθέσιμο, το script τερματίζει
    με σαφές μήνυμα αντί να πετάξει ακατέργαστο traceback.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000,
        )
        print(f"KafkaProducer OK (bootstrap_servers={bootstrap_servers})")
        return producer
    except KafkaError as e:
        print(f"[ΣΦΑΛΜΑ] Αποτυχία σύνδεσης στο Kafka/Redpanda: {e}")
        print("Βεβαιωθείτε ότι το Redpanda τρέχει (π.χ. docker compose up -d) "
              "και ότι το port είναι σωστό (external port, π.χ. 19092).")
        sys.exit(1)
    except Exception as e:
        print(f"[ΣΦΑΛΜΑ] Απρόσμενο σφάλμα κατά τη δημιουργία του producer: {e}")
        sys.exit(1)


def kafka_producer_loop(W, topic="uxsim", bootstrap_servers="localhost:19092", interval=1.0):
    """
    interval: Ν σε δευτερόλεπτα ανάμεσα σε διαδοχικά sends (παραμετροποίηση Ν
    όπως ζητά η εκφώνηση, μέσω του --interval command-line argument).
    """
    producer = create_producer(bootstrap_servers)

    # Run full simulation and convert all vehicle trajectories to a DataFrame
    W.exec_simulation()
    # print(dir(W)) # You can check what the world contains
    df = W.analyzer.vehicles_to_pandas()
    print(df.columns.tolist())
    print(df.head(2))

    # Φιλτράρισμα: κρατάμε μόνο τα οχήματα που βρίσκονται πραγματικά σε κίνηση (speed > 0),
    # όπως ζητά ρητά η εκφώνηση (Ερώτημα 1, βήμα 3.4).
    speed_col = 'v'
    before = len(df)
    df = df[df[speed_col] > 0]
    after = len(df)
    print(f"Φιλτράρισμα speed > 0: {before} -> {after} εγγραφές")

    # Replay timeline. Use the correct time column (usually 't')
    time_col = 't'
    times = sorted(df[time_col].unique())

    for t in times:
        snapshot = df[df[time_col] == t]
        # Stream the rows to Kafka in a loop:
        for _, row in snapshot.iterrows():
            record = {k: convert(v) for k, v in row.to_dict().items()}
            try:
                print(f"Sending to topic '{topic}':", record)
                producer.send(topic, record)  # producer.send() is asynchronous. Kafka doesn't immediately shoot that data across the network
            except KafkaError as e:
                print(f"[ΠΡΟΕΙΔΟΠΟΙΗΣΗ] Αποτυχία αποστολής record: {e}")

        try:
            producer.flush()  # "Stop waiting for more messages. Empty the buffer and send everything to the broker right now."
        except KafkaError as e:
            print(f"[ΠΡΟΕΙΔΟΠΟΙΗΣΗ] Αποτυχία flush: {e}")

        print(" ---------- flushed ----------\n")
        # Add a delay to simulate real‑time (DELTAT: simulation time step Δt)
        time.sleep(interval)   # παραμετροποιημένο Ν, ή Δt από UXSIM # time.sleep(W.DELTAT)

    producer.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="UXSIM -> Kafka/Redpanda producer (streaming traffic data)."
    )
    parser.add_argument(
        "--interval", "-n", type=float, default=1.0,
        help="Ν: διάστημα σε δευτερόλεπτα ανάμεσα σε διαδοχικές αποστολές (default: 1.0)"
    )
    parser.add_argument(
        "--topic", type=str, default="uxsim",
        help="Όνομα του Kafka/Redpanda topic (default: uxsim)"
    )
    parser.add_argument(
        "--bootstrap-servers", type=str, default="localhost:19092",
        help="Kafka/Redpanda bootstrap servers (default: localhost:19092)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    W = simulate_traffic()
    kafka_producer_loop(
        W,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        interval=args.interval,
    )
    print("Process completed.")