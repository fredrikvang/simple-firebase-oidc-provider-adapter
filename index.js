const Firestore = require("@google-cloud/firestore")

const db = new Firestore()

class FirestoreAdapter {
  constructor(model) {
    this.model = `oicd-provider-${model}`
  }

  async destroy(id) {
    const batch = db.batch()
    await this._batched_destroy(id, batch)
    await batch.commit()
  }

  async _batched_destroy(id, batch) {
    const doc = await db.collection(this.model).doc(id).get()
    if (doc.exists) {
      const data = await doc.data()
      if (data.userCode) {
        batch.delete(db.collection(this.model).doc("userCode::" + data.userCode))
      }
      if (data.uid) {
        batch.delete(db.collection(this.model).doc("uid::" + data.uid))
      }
      batch.delete(db.collection(this.model).doc(id))
    }
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
    const doc = await db
      .collection(this.model)
      .doc("uid::" + uid)
      .get()
    if (!doc.exists) return undefined
    const data = await doc.data()
    return this.find(data.id)
  }

  async findByUserCode(userCode) {
    const doc = await db
      .collection(this.model)
      .doc("userCode::" + userCode)
      .get()
    if (!doc.exists) return undefined
    const data = await doc.data()
    return this.find(data.id)
  }

  async upsert(id, payload, expiresIn) {
    const batch = db.batch()
    batch.set(db.collection(this.model).doc(id), {
      ...payload,
      __expiry: Date.now() + expiresIn * 1000,
    })
    if (payload.uid) {
      batch.set(db.collection(this.model).doc("uid::" + payload.uid), { id })
    }
    if (payload.userCode) {
      batch.set(db.collection(this.model).doc("userCode::" + payload.userCode), {
        id,
      })
    }
    batch.commit()
  }

  async revokeByGrantId(grantId) {
    const coll = await db
      .collection(this.model)
      .where("grantId", "==", grantId)
      .get()
    const ids = coll.map((x) => x.id)
    const batch = db.batch()
    for (id of ids) this._batched_destroy(id, batch)
    await batch.commit()
  }


}

module.exports = FirestoreAdapter
