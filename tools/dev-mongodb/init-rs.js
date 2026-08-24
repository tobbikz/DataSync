// Init single-node replica set + sample docs (boolean + datetime fields for coercion audit)
function waitPrimary() {
  for (let i = 0; i < 60; i++) {
    try {
      const s = rs.status();
      if (s.ok && s.members && s.members.some((m) => m.stateStr === "PRIMARY")) {
        return;
      }
    } catch (e) {
      /* not initiated yet */
    }
    sleep(1000);
  }
  throw new Error("replica set did not elect PRIMARY in time");
}

try {
  rs.status();
} catch (e) {
  rs.initiate({
    _id: "rs0",
    members: [{ _id: 0, host: "127.0.0.1:27017" }],
  });
}

waitPrimary();

const testdb = db.getSiblingDB("mongotest");
testdb.customers.deleteMany({});
testdb.orders_probe.deleteMany({});

testdb.customers.insertMany([
  { name: "Dev Customer", active: true, updated_at: new Date() },
  { name: "Dev Customer 2", active: false, updated_at: new Date() },
]);

testdb.orders_probe.insertOne({
  sku: "SMOKE-SKU",
  qty: 2,
  created_at: new Date(),
});

print("mongo dev init ok");
