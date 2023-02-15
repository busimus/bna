-- Table: public.Transfers

-- DROP TABLE IF EXISTS public."Transfers";

CREATE TABLE IF NOT EXISTS public."Transfers"
(
    blocknum integer NOT NULL,
    logindex smallint NOT NULL,
    "time" timestamp with time zone NOT NULL,
    data jsonb NOT NULL,
    CONSTRAINT "Transfers_pkey" PRIMARY KEY (blocknum, logindex)
)

TABLESPACE pg_default;

-- DROP INDEX IF EXISTS public.transfer_time_index;

CREATE INDEX IF NOT EXISTS transfer_time_index
    ON public."Transfers" USING btree
    ("time" ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: public.Trades

-- DROP TABLE IF EXISTS public."Trades";

CREATE TABLE IF NOT EXISTS public."Trades"
(
    id bigint NOT NULL,
    market character varying(16) COLLATE pg_catalog."default" NOT NULL,
    "time" timestamp with time zone NOT NULL,
    data jsonb NOT NULL,
    CONSTRAINT "Trades_pkey" PRIMARY KEY (id, market)
)

TABLESPACE pg_default;

-- DROP INDEX IF EXISTS public.trade_time_index;

CREATE INDEX IF NOT EXISTS trade_time_index
    ON public."Trades" USING btree
    ("time" ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: public.Identities

-- DROP TABLE IF EXISTS public."Identities";

CREATE TABLE IF NOT EXISTS public."Identities"
(
    address character(42) COLLATE pg_catalog."default" NOT NULL,
    fetch_time timestamp with time zone NOT NULL,
    data jsonb,
    CONSTRAINT "Identities_pkey" PRIMARY KEY (address)
)

TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public."Events"
(
    id bigint NOT NULL,
    channel bigint NOT NULL,
    message bigint NOT NULL,
    -- time timestamp with time zone NOT NULL,
    event jsonb,
    CONSTRAINT "Events_pkey" PRIMARY KEY (id)
)

TABLESPACE pg_default;
