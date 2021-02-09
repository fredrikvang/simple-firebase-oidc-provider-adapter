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
    const snapshot = await db.collection(this.model).where('uid', '==', uid).get()
    if (snapshot.empty) {
      return undefined
    } else {
      if (snapshot.size !== 1) {
        console.warn(`Querying ${this.model} for userCode ${uid} returned ${snapshot.size} items (instead of exactly 1)`)
      }
      return snapshot.docs[0].data
    }
  }

  async findByUserCode(userCode) {
    const snapshot = await db.collection(this.model).where('userCode', '==', userCode).get()
    if (snapshot.empty) {
      return undefined
    } else {
      if (snapshot.size !== 1) {
        console.warn(`Querying ${this.model} for userCode ${userCode} returned ${snapshot.size} items (instead of exactly 1)`)
      }
      return snapshot.docs[0].data
    }
  }

  async upsert(id, payload, expiresIn) {
    await db.collection(this.model).doc(id).set({
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

module.exports = FirestoreAdapter
