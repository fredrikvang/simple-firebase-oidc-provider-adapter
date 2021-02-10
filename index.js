const Firestore = require("@google-cloud/firestore")

const db = new Firestore()

class FirestoreAdapter {
  constructor(model) {
    this.model = `oidc-provider-${model}`
  }

  async destroy(id) {
    await db.collection(this.model).doc(id).delete()
  }

  async consume(id) {
    const doc = await db.collection(this.model).doc(id).get()
    if (doc.exists) {
      await db.collection(this.model).doc(id).update({ consumed: Date.now() })
    }
  }

  async find(id) {
    const doc = await db.collection(this.model).doc(id).get()
    if (!doc.exists) return undefined
    const data = await doc.data()
    const expired = doc.__expiry < Date.now()
    if (expired) {
      return undefined
    } else {
      return data
    }
  }

  async findByUid(uid) {
    const snapshot = await db
      .collection(this.model)
      .where("uid", "==", uid)
      .get()
    if (snapshot.empty) {
      return undefined
    } else {
      if (snapshot.size !== 1) {
        console.warn(
          `Querying ${this.model} for userCode ${uid} returned ${snapshot.size} items (instead of exactly 1)`
        )
      }
      return snapshot.docs[0].data
    }
  }

  async findByUserCode(userCode) {
    const snapshot = await db
      .collection(this.model)
      .where("userCode", "==", userCode)
      .get()
    if (snapshot.empty) {
      return undefined
    } else {
      if (snapshot.size !== 1) {
        console.warn(
          `Querying ${this.model} for userCode ${userCode} returned ${snapshot.size} items (instead of exactly 1)`
        )
      }
      return snapshot.docs[0].data
    }
  }

  async upsert(id, payload, expiresIn) {
    await db
      .collection(this.model)
      .doc(id)
      .set({
        ...payload,
        __expiry: Date.now() + expiresIn * 1000,
      })
  }

  async revokeByGrantId(grantId) {
    const snapshot = await db
      .collection(this.model)
      .where("grantId", "==", grantId)
      .get()
    const ids = snapshot.docs.map((x) => x.id)
    const batch = db.batch()
    for (id of ids) batch.delete(db.collection(this.collection).doc(id))
    await batch.commit()
  }
}

/**
 * Utility function to clean up any expired items. Should be called regularly.
 */
const deleteExpiredEntries = async function () {
  const allCollections = await db.listCollections()
  const oidcCollections = allCollections
    .map((x) => x.id)
    .filter((x) => /^oidc-provider-/.test(x))
  const limit = 50
  // FOr each collection beginning with 'oidc-provider-'
  for (collection of oidcCollections) {
    // Repeat until we retrieve less than 'limit' expired items from the database
    while (true) {
      // Get at most 'limit' expired items
      const docs = await db
        .collection(collection)
        .where("__expiry", "<", Date.now())
        .limit(limit)
        .get()
      const retrievedItems = docs.size
      if (retrievedItems > 0) { // If any were retrieved
        const batch = db.batch() // start a batch operation
        docs.forEach((x) => batch.delete(db.collection(collection).doc(x.id)))
        await batch.commit() // Where each collection->id is deleted.
      }
      if (retrievedItems < limit) break // Nothing more to retrieve, break the while loop
    }
  }
}

module.exports = { FirestoreAdapter, deleteExpiredEntries }
