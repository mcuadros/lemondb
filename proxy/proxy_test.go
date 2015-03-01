package proxy

import (
	"testing"
	"time"

	"github.com/mcuadros/exmongodb/middlewares"

	"github.com/facebookgo/mgotest"
	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func Test(t *testing.T) {
	Suite(&ProxySuite{testing: t})
	TestingT(t)
}

type ProxySuite struct {
	testing *testing.T
	server  *mgotest.Server
	session *mgo.Session
	proxy   *Proxy
}

func (s *ProxySuite) SetUpTest(c *C) {
	s.server = mgotest.NewStartedServer(s.testing)
	s.proxy = s.getNewProxy(s.server.URL())
	s.proxy.Start()

	s.session, _ = mgo.Dial(s.proxy.ProxyAddr)
}

func (s *ProxySuite) TestProxy_SimpleCRUD(c *C) {
	collection := s.session.DB("test").C("coll1")
	data := map[string]interface{}{
		"_id":  1,
		"name": "abc",
	}
	err := collection.Insert(data)
	c.Assert(err, IsNil)

	n, err := collection.Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	result := make(map[string]interface{})
	collection.Find(bson.M{"_id": 1}).One(&result)
	c.Assert(result["name"], Equals, "abc")

	err = collection.DropCollection()
	c.Assert(n, Equals, 1)
}

func (s *ProxySuite) TestProxy_IDConstraint(c *C) {
	collection := s.session.DB("test").C("coll1")
	data := map[string]interface{}{
		"_id":  1,
		"name": "abc",
	}

	err := collection.Insert(data)
	c.Assert(err, IsNil)

	err = collection.Insert(data)
	c.Assert(err, NotNil)
}

// inserting data voilating index clause on a separate connection should fail
func (s *ProxySuite) TestProxy_EnsureIndex(c *C) {
	collection := s.session.DB("test").C("coll1")
	index := mgo.Index{
		Key:        []string{"lastname", "firstname"},
		Unique:     true,
		DropDups:   true,
		Background: true, // See notes.
		Sparse:     true,
	}
	err := collection.EnsureIndex(index)
	c.Assert(err, IsNil)

	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	c.Assert(err, IsNil)

	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	c.Assert(err, NotNil)
}

// inserting same data after dropping an index should work
func (s *ProxySuite) TestProxy_DropIndex(c *C) {
	collection := s.session.DB("test").C("coll1")
	index := mgo.Index{
		Key:        []string{"lastname", "firstname"},
		Unique:     true,
		DropDups:   true,
		Background: true, // See notes.
		Sparse:     true,
	}
	err := collection.EnsureIndex(index)
	c.Assert(err, IsNil)

	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	c.Assert(err, IsNil)

	err = collection.DropIndex("lastname", "firstname")
	c.Assert(err, IsNil)

	err = collection.Insert(
		map[string]string{
			"firstname": "harvey",
			"lastname":  "dent",
		},
	)
	c.Assert(err, IsNil)

}

func (s *ProxySuite) TestProxy_Remove(c *C) {
	collection := s.session.DB("test").C("coll1")
	err := collection.Insert(bson.M{"S": "hello", "I": 24})
	c.Assert(err, IsNil)

	err = collection.Remove(bson.M{"S": "hello", "I": 24})
	c.Assert(err, IsNil)

	var res []interface{}
	collection.Find(bson.M{"S": "hello", "I": 24}).All(&res)
	c.Assert(res, IsNil)

	err = collection.Remove(bson.M{"S": "hello", "I": 24})
	c.Assert(res, IsNil)
}

func (s *ProxySuite) TestProxy_Update(c *C) {
	collection := s.session.DB("test").C("coll1")
	err := collection.Insert(bson.M{"_id": "1234", "name": "Alfred"})
	c.Assert(err, IsNil)

	var result map[string]interface{}
	collection.Find(nil).One(&result)
	c.Assert(result["name"], Equals, "Alfred")

	err = collection.Update(bson.M{"_id": "1234"}, bson.M{"name": "Jeeves"})
	c.Assert(err, IsNil)

	collection.Find(nil).One(&result)
	c.Assert(result["name"], Equals, "Jeeves")

	collection.Update(bson.M{"_id": "00000"}, bson.M{"name": "Jeeves"})
	c.Assert(err, IsNil)
}

func (s *ProxySuite) TestProxy_GoingAwayAndReturning(c *C) {
	collection := s.session.DB("test").C("coll1")
	err := collection.Insert(bson.M{"value": 1})
	c.Assert(err, IsNil)

	s.server.Stop()
	s.server.Start()

	// For now we can only gurantee that eventually things will work again. In an
	// ideal world the very first client connection after mongo returns should
	// work, and we shouldn't need a loop here.
	for {
		collection = s.session.Copy().DB("test").C("coll1")
		if err := collection.Insert(bson.M{"value": 3}); err == nil {
			break
		}
	}
}

func (s *ProxySuite) TearDownTest(c *C) {
	s.session.Close()
	s.proxy.Stop()
	s.server.Stop()
}

func (s *ProxySuite) getNewProxy(mongoAddr string) *Proxy {
	return &Proxy{
		Log:               nopLogger{},
		ProxyAddr:         "localhost:2000",
		MongoAddr:         mongoAddr,
		ClientIdleTimeout: 5 * time.Minute,
		MessageTimeout:    5 * time.Second,
		Middleware:        &middlewares.ProxyMiddleware{},
	}
}

type nopLogger struct{}

func (n nopLogger) Error(args ...interface{})                 {}
func (n nopLogger) Errorf(format string, args ...interface{}) {}
func (n nopLogger) Warn(args ...interface{})                  {}
func (n nopLogger) Warnf(format string, args ...interface{})  {}
func (n nopLogger) Info(args ...interface{})                  {}
func (n nopLogger) Infof(format string, args ...interface{})  {}
func (n nopLogger) Debug(args ...interface{})                 {}
func (n nopLogger) Debugf(format string, args ...interface{}) {}
