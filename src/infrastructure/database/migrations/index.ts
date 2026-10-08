import { Migration20261008000001CoreSchema } from "./Migration20261008000001CoreSchema";
import { Migration20261009000001DoubleEntryJournal } from "./Migration20261009000001DoubleEntryJournal";
import { Migration20261010000001SubmissionFailureAudit } from "./Migration20261010000001SubmissionFailureAudit";

/** Explicit, ordered list: no filesystem globbing, identical behaviour in dev, tests and containers. */
export const MIGRATIONS = [
  { name: "Migration20261008000001CoreSchema", class: Migration20261008000001CoreSchema },
  { name: "Migration20261009000001DoubleEntryJournal", class: Migration20261009000001DoubleEntryJournal },
  {
    name: "Migration20261010000001SubmissionFailureAudit",
    class: Migration20261010000001SubmissionFailureAudit,
  },
];
