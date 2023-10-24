/* IMPORTANT
 * This snapshot file is auto-generated, but designed for humans.
 * It should be checked into source control and tracked carefully.
 * Re-generate by setting TAP_SNAPSHOT=1 and running tests.
 * Make sure to inspect the output below.  Do not ignore changes!
 */
'use strict'
exports[`tests/use/webhook/index.test.ts TAP submit tasks on event > must match snapshot 1`] = `
Array [
  Object {
    "d": Object {
      "data": Object {
        "text": "abc ",
      },
      "tn": "task1",
      "trace": Object {
        "event_name": "test_event",
        "t_id": "123",
        "type": "event",
      },
    },
    "eis": 300,
    "q": "emit_event_queue",
    "r_b": false,
    "r_d": 5,
    "r_l": 3,
    "saf": 0,
    "skey": null,
  },
  Object {
    "d": Object {
      "data": Object {
        "text": "abc ",
      },
      "tn": "task_2",
      "trace": Object {
        "event_name": "test_event",
        "t_id": "123",
        "type": "event",
      },
    },
    "eis": 300,
    "q": "emit_event_queue",
    "r_b": false,
    "r_d": 5,
    "r_l": 3,
    "saf": 0,
    "skey": null,
  },
  Object {
    "d": Object {
      "data": Object {
        "rrr": "123",
      },
      "tn": "task_3",
      "trace": Object {
        "event_name": "awdawd",
        "t_id": "5a4wdawdawd",
        "type": "event",
      },
    },
    "eis": 300,
    "q": "emit_event_queue",
    "r_b": false,
    "r_d": 5,
    "r_l": 3,
    "saf": 0,
    "skey": null,
  },
]
`
