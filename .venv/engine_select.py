def main():
    stmt = select()
    with engine.connect() as conn:
        cur = conn.execute(stmt)

        for res in cur:
            print(res)

if __name__ == "__main__":
    main()