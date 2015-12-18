package mgolock

import (
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type CantLockErr int

func (lle *CantLockErr) Error() string {
	return "Can't lock the requested document"
}

//the lock struct
type lock struct {
	ets time.Time     `bson:"ets"`
	id  bson.ObjectId `bson:"_id"`
}

//a mutex locked collection
type LockedC struct {
	c     *mgo.Collection
	locks map[interface{}]bson.ObjectId
}

func MakeLockedC(c *mgo.Collection) *LockedC {
	return &LockedC{c: c, locks: map[interface{}]bson.ObjectId{}}
}

//tries to lock document identified with id with a d timeout, populates result with the document
func (lc *LockedC) GetIdLocked(id interface{}, d time.Duration, result interface{}) error {
	if lc.c == nil {
		return errors.New("Collection not defined")
	}
	//if not exists, cache a lock id for this id so we can reuse and reissue the lock
	lockId, ok := lc.locks[id]
	if !ok {
		lockId = bson.NewObjectId()
		lc.locks[id] = lockId
	}
	//create a new lock
	now := time.Now()
	l := lock{
		ets: now.Add(d),
		id:  lockId,
	}
	change := mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"_mgolock": bson.M{"_id": l.id, "ets": l.ets},
			},
		},
		ReturnNew: true,
	}
	_, err := lc.c.Find(bson.M{
		"_id": id,
		"$or": []bson.M{
			bson.M{"_mgolock": bson.M{"$exists": false}},
			bson.M{"_mgolock.ets": bson.M{"$lt": now}},
			bson.M{"_mgolock._id": lockId},
		},
	}).Apply(change, result)
	return err
}

//updates a locked document if lock is still ours, this must be a SAFE update(with update operators),
//otherwise the lock will be overwritten (the updatee will go ok but you'll lose the lock)
func (lc *LockedC) UpdateId(id interface{}, update interface{}) error {
	if lc.c == nil {
		return errors.New("Collection not defined")
	}
	lockId, ok := lc.locks[id]
	if !ok {
		return errors.New("Never seen this id before")
	}
	err := lc.c.Update(bson.M{"_id": id, "_mgolock._id": lockId}, update)
	if err != nil {
		return lc.parseMgoError(id, err)
	}
	return nil
}

//unlocks a locked document if lock is still ours
func (lc *LockedC) Unlock(id interface{}) error {
	lockId, gotKey := lc.locks[id]
	if !gotKey {
		return errors.New("Never seen this id before")
	}
	err := lc.c.Update(bson.M{"_id": id, "_mgolock._id": lockId}, bson.M{"$unset": bson.M{"_mgolock": ""}})
	if err != nil {
		return lc.parseMgoError(id, err)
	}
	return nil
}

//returns a more meaningful error, distinguishing between lock and mongo errors
func (lc *LockedC) parseMgoError(id interface{}, err error) error {
	if err == mgo.ErrNotFound {
		//did we lose the lock or document no more?!
		n, e := lc.c.FindId(id).Count()
		if e != nil { //shit happens
			return e
		}
		if n > 0 { //yeah we lost the lock
			var cle CantLockErr
			return &cle
		} else { //not our fault, no document no more, clean the cached id
			delete(lc.locks, id)
			return err
		}
	}
	//that's a mongo error
	return err
}
