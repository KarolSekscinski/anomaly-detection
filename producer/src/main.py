from utils.ProducerClass import Producer

producer = Producer()


def main() -> None:
    """
    TODO: update docstring

    """
    producer.run_server()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
