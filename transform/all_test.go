package transforms

import (
	"reflect"
	"testing"
)

func TestAll(t *testing.T) {
	var k, v interface{}
	k = "1"
	v = `{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": "12.456"}}`
	transforms := []Transformer{
		&Cast{`Cast$Value`, []CastProps{{"height", "float32"}}},
		&ExtractField{"ExtractField&Value", "height"},
		&Cast{`Cast$Value`, []CastProps{{"", "string"}}},
		&Cast{`Cast$Value`, []CastProps{{"", "int32"}}},
	}

	rec := NewRec(k, v, "", 0)
	for _, trans := range transforms {
		rec = trans.Transform(rec)
	}

	t.Log(rec.Key(), rec.Value(), reflect.TypeOf(rec.Value()).String())
}

/*
   "transforms": "Cast,InsertField,ReplaceField",
   "transforms.Cast.type": "Cast$Value",
   "transforms.Cast.spec": "followers:string,public_repos:string",
   "transforms.InsertField.type": "InsertField$Value",
   "transforms.InsertField.static.field": "age",
   "transforms.InsertField.static.value": 100,
   "transforms.ReplaceField.type": "ReplaceField$Value",
   "transforms.ReplaceField.whitelist": "name,company,blog,location,email,followers,public_repos",
   "transforms.ReplaceField.renames": "name:user.name,company:other.company,blog:user.blog,location:other.location,email:user.email,followers:other.followers,public_repos:user.repository_count,age:user.Age",
*/
func TestTransform1(t *testing.T) {

	reg := NewReg()
	cfg := make(map[string]interface{})
	cfg[`transforms`] = `Cast,InsertField,ReplaceField,ExtractTopic`
	cfg[`transforms.Cast.type`] = `Cast$Value`
	cfg[`transforms.Cast.spec`] = `followers:string,public_repos:string`
	cfg[`transforms.InsertField.type`] = `InsertField$Value`
	cfg[`transforms.InsertField.static.field`] = `age`
	cfg[`transforms.InsertField.static.value`] = 100
	cfg[`transforms.ReplaceField.type`] = `ReplaceField$Value`
	cfg[`transforms.ReplaceField.whitelist`] = `name,company,blog,location,email,followers,public_repos,age`
	cfg[`transforms.ReplaceField.renames`] = `name:user.name,company:other.company,blog:user.blog,location:other.location,email:user.email,followers:other.followers,public_repos:user.repository_count,age:user.age`
	cfg[`transforms.ExtractTopic.type`] = `ExtractTopic$Value`
	cfg[`transforms.ExtractTopic.field`] = `company`
	trans := reg.Init(cfg)

	jsonStr := `{"avatar_url":"https://github.com/images/error/octocat_happy.gif","bio":"There once was...","blog":"https://github.com/blog","company":"GitHub","created_at":"2008-01-14T04:33:35Z","email":"octocat@github.com","events_url":"https://api.github.com/users/octocat/events{/privacy}","followers":20,"followers_url":"https://api.github.com/users/octocat/followers","following":0,"following_url":"https://api.github.com/users/octocat/following{/other_user}","gists_url":"https://api.github.com/users/octocat/gists{/gist_id}","gravatar_id":"","hireable":false,"html_url":"https://github.com/octocat","id":1,"location":"San Francisco","login":"octocat","name":"noel yahan","node_id":"MDQ6VXNlcjE=","organizations_url":"https://api.github.com/users/octocat/orgs","public_gists":1,"public_repos":2,"received_events_url":"https://api.github.com/users/octocat/received_events","repos_url":"https://api.github.com/users/octocat/repos","site_admin":false,"starred_url":"https://api.github.com/users/octocat/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/octocat/subscriptions","type":"User","updated_at":"2008-01-14T04:33:35Z","url":"https://api.github.com/users/octocat"}`
	rec := NewRec(`100`, jsonStr, ``, 0)
	for _, t := range trans {
		rec = t.Transform(rec)
	}
	t.Log(rec.Topic(), rec.Value())
}
