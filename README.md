# nbb.deps visibility

When running via `node` the `nbb.edn` dependencies cannot be located.

```bash
npm install 
npx nbb
node card-get.mjs
```

You will see something like this:

```bash
file:///home/ray/fc/repos/card-main/node_modules/nbb/lib/nbb_core.js:2214
2,null),new $APP.G(null,jt,new $APP.G(null,new $APP.G(null,$APP.Ul,new $APP.G(null,D,null,1,null),2,null),null,1,null),2,null),3,null),4,null),5,null),6,null)):null}finally{$APP.Wq()}}).then(function(){var P=$APP.z(a);return hx.h?hx.h(P,b):hx.call(null,P,b)}):Promise.reject(Error(["Could not find namespace: ",$APP.p.g(q)].join("")))}return Promise.resolve($APP.U.g(b))};
                                                                                                                                                                                                                                                                                   ^

Error: Could not find namespace: camel-snake-kebab.core
    at hx (file:///home/ray/fc/repos/card-main/node_modules/nbb/lib/nbb_core.js:2214:276)
    at file:///home/ray/fc/repos/card-main/node_modules/nbb/lib/nbb_core.js:2214:244
    at async file:///home/ray/fc/repos/card-main/card-get.mjs:5:21
```

