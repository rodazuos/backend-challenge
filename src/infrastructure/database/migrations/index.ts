import { Migration20261008000001CoreSchema } from "./Migration20261008000001CoreSchema";
import { Migration20261009000001DoubleEntryJournal } from "./Migration20261009000001DoubleEntryJournal";

/** Explicit, ordered list: no filesystem globbing, identical behaviour in dev, tests and containers. */
export const MIGRATIONS = [
  { name: "Migration20261008000001CoreSchema", class: Migration20261008000001CoreSchema },
  { name: "Migration20261009000001DoubleEntryJournal", class: Migration20261009000001DoubleEntryJournal },
];
