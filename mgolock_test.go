package mgolock

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

var db *mgo.Database

type mockObj struct {
	Id    bson.ObjectId `bson:"_id"`
	Updme string        `bson:"updme"`
}

func init() {
	//set up db
	session, err := mgo.Dial("127.0.0.1:27017")
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	db = session.DB("mgolock_test")
	db.DropDatabase()
}

func TestNormalUpdate(t *testing.T) {
	require := require.New(t)
	//insert mock obj
	m := mockObj{Id: bson.NewObjectId(), Updme: "hello"}
	err := db.C("tests").Insert(m)
	require.Nil(err)
	lc := MakeLockedC(db.C("tests")) //get our locked collection
	ld := mockObj{}
	err = lc.GetIdLocked(m.Id, 2*time.Second, &ld)
	require.Nil(err)
	require.Equal(m.Id, ld.Id)
	//again to ensure we can relock
	err = lc.GetIdLocked(m.Id, 2*time.Second, &ld)
	require.Nil(err)
	require.Equal(m.Id, ld.Id)
	//and relock again after timeout
	time.Sleep(3 * time.Second)
	err = lc.GetIdLocked(m.Id, 2*time.Second, &ld)
	require.Nil(err)
	require.Equal(m.Id, ld.Id)
	//try update
	err = lc.UpdateId(ld.Id, bson.M{"$set": bson.M{"updme": "world"}})
	require.Nil(err)
	m.Updme = "world"
	err = lc.GetIdLocked(m.Id, 2*time.Second, &ld)
	require.Nil(err)
	require.Exactly(m, ld)
	err = lc.Unlock(ld.Id)
	require.Nil(err)
	raw := map[string]interface{}{}
	err = db.C("tests").FindId(ld.Id).One(&raw)
	require.Nil(err)
	_, lockThere := raw["_mgolock"]
	require.False(lockThere)
	err = db.C("tests").FindId(ld.Id).One(&m)
	require.Nil(err)
	require.Exactly(ld, m)
}

func TestRaceUpdate(t *testing.T) {
	require := require.New(t)
	//insert mock obj
	m := mockObj{Id: bson.NewObjectId(), Updme: "hello"}
	err := db.C("tests").Insert(m)
	require.Nil(err)
	lc := MakeLockedC(db.C("tests")) //get our locked collection
	ld := mockObj{}
	err = lc.GetIdLocked(m.Id, 2*time.Second, &ld)
	require.Nil(err)
	//make another lockedC
	raceLc := MakeLockedC(db.C("tests")) //get our locked collection
	raceLd := mockObj{}
	err = raceLc.GetIdLocked(m.Id, 2*time.Second, &raceLd)
	require.NotNil(err)
}
