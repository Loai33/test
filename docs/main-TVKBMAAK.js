var mE=Object.defineProperty,gE=Object.defineProperties;var vE=Object.getOwnPropertyDescriptors;var vg=Object.getOwnPropertySymbols;var yE=Object.prototype.hasOwnProperty,_E=Object.prototype.propertyIsEnumerable;var yg=(n,e,t)=>e in n?mE(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,St=(n,e)=>{for(var t in e||={})yE.call(e,t)&&yg(n,t,e[t]);if(vg)for(var t of vg(e))_E.call(e,t)&&yg(n,t,e[t]);return n},Vt=(n,e)=>gE(n,vE(e));var nn=null,tc=!1,nf=1,xE=null,rn=Symbol("SIGNAL");function De(n){let e=nn;return nn=n,e}function ic(){return nn}var Ms={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Mo(n){if(tc)throw new Error("");if(nn===null)return;nn.consumerOnSignalRead(n);let e=nn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=nn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:nn.producers,t!==void 0&&t.producer===n)){nn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===nn&&(!i||bE(r,nn)))return;let s=Ts(nn),o={producer:n,consumer:nn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};nn.producersTail=o,e!==void 0?e.nextProducer=o:nn.producers=o,s&&bg(n,o)}function _g(){nf++}function rf(n){if(!(Ts(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===nf)){if(!n.producerMustRecompute(n)&&!To(n)){tf(n);return}n.producerRecomputeValue(n),tf(n)}}function sf(n){if(n.consumers===void 0)return;let e=tc;tc=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||EE(i)}}finally{tc=e}}function of(){return nn?.consumerAllowSignalWrites!==!1}function EE(n){n.dirty=!0,sf(n),n.consumerMarkedDirty?.(n)}function tf(n){n.dirty=!1,n.lastCleanEpoch=nf}function Ss(n){return n&&xg(n),De(n)}function xg(n){n.producersTail=void 0,n.recomputing=!0}function So(n,e){De(e),n&&Eg(n)}function Eg(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Ts(n))do t=af(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function To(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(rf(t),i!==t.version))return!0}return!1}function kr(n){if(Ts(n)){let e=n.producers;for(;e!==void 0;)e=af(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function bg(n,e){let t=n.consumersTail,i=Ts(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)bg(r.producer,r)}function af(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Ts(e)){let s=e.producers;for(;s!==void 0;)s=af(s)}return t}function Ts(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function cf(n){xE?.(n)}function bE(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function lf(n,e){return Object.is(n,e)}function rc(n,e){let t=Object.create(ME);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(rf(t),Mo(t),t.value===nc)throw t.error;return t.value};return i[rn]=t,cf(t),i}var Qd=Symbol("UNSET"),ef=Symbol("COMPUTING"),nc=Symbol("ERRORED"),ME=Vt(St({},Ms),{value:Qd,dirty:!0,error:null,equal:lf,kind:"computed",producerMustRecompute(n){return n.value===Qd||n.value===ef},producerRecomputeValue(n){if(n.value===ef)throw new Error("");let e=n.value;n.value=ef;let t=Ss(n),i,r=!1;try{i=n.computation(),De(null),r=e!==Qd&&e!==nc&&i!==nc&&n.equal(e,i)}catch(s){i=nc,n.error=s}finally{So(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function SE(){throw new Error}var Mg=SE;function Sg(n){Mg(n)}function uf(n){Mg=n}var TE=null;function df(n,e){let t=Object.create(sc);t.value=n,e!==void 0&&(t.equal=e);let i=()=>Tg(t);return i[rn]=t,cf(t),[i,o=>wo(t,o),o=>wg(t,o)]}function Tg(n){return Mo(n),n.value}function wo(n,e){of()||Sg(n),n.equal(n.value,e)||(n.value=e,wE(n))}function wg(n,e){of()||Sg(n),wo(n,e(n.value))}var sc=Vt(St({},Ms),{equal:lf,value:void 0,kind:"signal"});function wE(n){n.version++,_g(),sf(n),TE?.(n)}var ff=Vt(St({},Ms),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function hf(n){if(n.dirty=!1,n.version>0&&!To(n))return;n.version++;let e=Ss(n);try{n.cleanup(),n.fn()}finally{So(n,e)}}function En(n){return typeof n=="function"}function oc(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var ac=oc(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Co(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var pn=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(En(i))try{i()}catch(s){e=s instanceof ac?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{Cg(s)}catch(o){e=e??[],o instanceof ac?e=[...e,...o.errors]:e.push(o)}}if(e)throw new ac(e)}}add(e){var t;if(e&&e!==this)if(this.closed)Cg(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Co(t,e)}remove(e){let{_finalizers:t}=this;t&&Co(t,e),e instanceof n&&e._removeParent(this)}};pn.EMPTY=(()=>{let n=new pn;return n.closed=!0,n})();var pf=pn.EMPTY;function cc(n){return n instanceof pn||n&&"closed"in n&&En(n.remove)&&En(n.add)&&En(n.unsubscribe)}function Cg(n){En(n)?n():n.unsubscribe()}var ei={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ws={setTimeout(n,e,...t){let{delegate:i}=ws;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=ws;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Ig(n){ws.setTimeout(()=>{let{onUnhandledError:e}=ei;if(e)e(n);else throw n})}function mf(){}var Ag=gf("C",void 0,void 0);function Dg(n){return gf("E",void 0,n)}function Rg(n){return gf("N",n,void 0)}function gf(n,e,t){return{kind:n,value:e,error:t}}var Ur=null;function Cs(n){if(ei.useDeprecatedSynchronousErrorHandling){let e=!Ur;if(e&&(Ur={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Ur;if(Ur=null,t)throw i}}else n()}function Ng(n){ei.useDeprecatedSynchronousErrorHandling&&Ur&&(Ur.errorThrown=!0,Ur.error=n)}var Br=class extends pn{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,cc(e)&&e.add(this)):this.destination=AE}static create(e,t,i){return new Is(e,t,i)}next(e){this.isStopped?yf(Rg(e),this):this._next(e)}error(e){this.isStopped?yf(Dg(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?yf(Ag,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},CE=Function.prototype.bind;function vf(n,e){return CE.call(n,e)}var _f=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){lc(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){lc(i)}else lc(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){lc(t)}}},Is=class extends Br{constructor(e,t,i){super();let r;if(En(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&ei.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&vf(e.next,s),error:e.error&&vf(e.error,s),complete:e.complete&&vf(e.complete,s)}):r=e}this.destination=new _f(r)}};function lc(n){ei.useDeprecatedSynchronousErrorHandling?Ng(n):Ig(n)}function IE(n){throw n}function yf(n,e){let{onStoppedNotification:t}=ei;t&&ws.setTimeout(()=>t(n,e))}var AE={closed:!0,next:mf,error:IE,complete:mf};var Pg=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Lg(n){return n}function Og(n){return n.length===0?Lg:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var As=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=RE(t)?t:new Is(t,i,r);return Cs(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=Fg(i),new i((r,s)=>{let o=new Is({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Pg](){return this}pipe(...t){return Og(t)(this)}toPromise(t){return t=Fg(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function Fg(n){var e;return(e=n??ei.Promise)!==null&&e!==void 0?e:Promise}function DE(n){return n&&En(n.next)&&En(n.error)&&En(n.complete)}function RE(n){return n&&n instanceof Br||DE(n)&&cc(n)}function NE(n){return En(n?.lift)}function kg(n){return e=>{if(NE(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Ug(n,e,t,i,r){return new xf(n,e,t,i,r)}var xf=class extends Br{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var Bg=oc(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Ui=(()=>{class n extends As{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new uc(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new Bg}next(t){Cs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Cs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Cs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?pf:(this.currentObservers=null,s.push(t),new pn(()=>{this.currentObservers=null,Co(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new As;return t.source=this,t}}return n.create=(e,t)=>new uc(e,t),n})(),uc=class extends Ui{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:pf}};var Io=class extends Ui{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function Ef(n,e){return kg((t,i)=>{let r=0;t.subscribe(Ug(i,s=>{i.next(n.call(e,s,r++))}))})}var bf;function dc(){return bf}function xi(n){let e=bf;return bf=n,e}var Vg=Symbol("NotFound");function Ds(n){return n===Vg||n?.name==="\u0275NotFound"}var _c="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Xe=class extends Error{code;constructor(e,t){super(Lo(e,t)),this.code=e}};function PE(n){return`NG0${Math.abs(n)}`}function Lo(n,e){return`${PE(n)}${e?": "+e:""}`}function mt(n){for(let e in n)if(n[e]===mt)return e;throw Error("")}function xc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(xc).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Ec(n,e){return n?e?`${n} ${e}`:n:e||""}var LE=mt({__forward_ref__:mt});function bc(n){return n.__forward_ref__=bc,n}function An(n){return jg(n)?n():n}function jg(n){return typeof n=="function"&&n.hasOwnProperty(LE)&&n.__forward_ref__===bc}function Ht(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Mc(n){return OE(n,Sc)}function OE(n,e){return n.hasOwnProperty(e)&&n[e]||null}function FE(n){let e=n?.[Sc]??null;return e||null}function Sf(n){return n&&n.hasOwnProperty(hc)?n[hc]:null}var Sc=mt({\u0275prov:mt}),hc=mt({\u0275inj:mt}),st=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Ht({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Ff(n){return n&&!!n.\u0275providers}var kf=mt({\u0275cmp:mt}),Uf=mt({\u0275dir:mt}),Bf=mt({\u0275pipe:mt});var Tf=mt({\u0275fac:mt}),Gr=mt({__NG_ELEMENT_ID__:mt}),Hg=mt({__NG_ENV_ID__:mt});function Wr(n){return Hf(n,"@Component"),n[kf]||null}function Vf(n){return Hf(n,"@Directive"),n[Uf]||null}function $g(n){return Hf(n,"@Pipe"),n[Bf]||null}function Hf(n,e){if(n==null)throw new Xe(-919,!1)}function jr(n){return typeof n=="string"?n:n==null?"":String(n)}var qg=mt({ngErrorCode:mt}),kE=mt({ngErrorMessage:mt}),UE=mt({ngTokenPath:mt});function zf(n,e){return Xg("",-200,e)}function Tc(n,e){throw new Xe(-201,!1)}function Xg(n,e,t){let i=new Xe(e,n);return i[qg]=e,i[kE]=n,t&&(i[UE]=t),i}function BE(n){return n[qg]}var wf;function Yg(){return wf}function In(n){let e=wf;return wf=n,e}function Gf(n,e,t){let i=Mc(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Tc(n,"")}var VE={},Vr=VE,HE="__NG_DI_FLAG__",Cf=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Hr(t)||0;try{return this.injector.get(e,i&8?null:Vr,i)}catch(r){if(Ds(r))return r;throw r}}};function zE(n,e=0){let t=dc();if(t===void 0)throw new Xe(-203,!1);if(t===null)return Gf(n,void 0,e);{let i=GE(e),r=t.retrieve(n,i);if(Ds(r)){if(i.optional)return null;throw r}return r}}function gt(n,e=0){return(Yg()||zE)(An(n),e)}function Ie(n,e){return gt(n,Hr(e))}function Hr(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function GE(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function If(n){let e=[];for(let t=0;t<n.length;t++){let i=An(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Xe(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=WE(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(gt(r,s))}else e.push(gt(i))}return e}function WE(n){return n[HE]}function Ns(n,e){let t=n.hasOwnProperty(Tf);return t?n[Tf]:null}function Zg(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Jg(n){return n.flat(Number.POSITIVE_INFINITY)}function wc(n,e){n.forEach(t=>Array.isArray(t)?wc(t,e):e(t))}function Wf(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Oo(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Kg(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function jf(n,e,t){let i=Ps(n,e);return i>=0?n[i|1]=t:(i=~i,Kg(n,i,e,t)),i}function Cc(n,e){let t=Ps(n,e);if(t>=0)return n[t|1]}function Ps(n,e){return jE(n,e,1)}function jE(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var $r={},ti=[],Ls=new st(""),$f=new st("",-1),qf=new st(""),Do=class{get(e,t=Vr){if(t===Vr){let r=Xg("",-201);throw r.name="\u0275NotFound",r}return t}};function Ic(n){return{\u0275providers:n}}function Qg(n){return Ic([{provide:Ls,multi:!0,useValue:n}])}function e0(...n){return{\u0275providers:Xf(!0,n),\u0275fromNgModule:!0}}function Xf(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return wc(e,o=>{let a=o;pc(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&t0(r,s),t}function t0(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Yf(r,s=>{e(s,i)})}}function pc(n,e,t,i){if(n=An(n),!n)return!1;let r=null,s=Sf(n),o=!s&&Wr(n);if(!s&&!o){let c=n.ngModule;if(s=Sf(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)pc(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;wc(s.imports,u=>{pc(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&t0(l,e)}if(!a){let l=Ns(r)||(()=>new r);e({provide:r,useFactory:l,deps:ti},r),e({provide:qf,useValue:r,multi:!0},r),e({provide:Ls,useValue:()=>gt(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;Yf(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Yf(n,e){for(let t of n)Ff(t)&&(t=t.\u0275providers),Array.isArray(t)?Yf(t,e):e(t)}var $E=mt({provide:String,useValue:mt});function n0(n){return n!==null&&typeof n=="object"&&$E in n}function qE(n){return!!(n&&n.useExisting)}function XE(n){return!!(n&&n.useFactory)}function mc(n){return typeof n=="function"}var Fo=new st(""),fc={},zg={},Mf;function ko(){return Mf===void 0&&(Mf=new Do),Mf}var Hn=class{},zr=class extends Hn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Df(e,o=>this.processProvider(o)),this.records.set($f,Rs(void 0,this)),r.has("environment")&&this.records.set(Hn,Rs(void 0,this));let s=this.records.get(Fo);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(qf,ti,{self:!0}))}retrieve(e,t){let i=Hr(t)||0;try{return this.get(e,Vr,i)}catch(r){if(Ds(r))return r;throw r}}destroy(){Ao(this),this._destroyed=!0;let e=De(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),De(e)}}onDestroy(e){return Ao(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Ao(this);let t=xi(this),i=In(void 0),r;try{return e()}finally{xi(t),In(i)}}get(e,t=Vr,i){if(Ao(this),e.hasOwnProperty(Hg))return e[Hg](this);let r=Hr(i),s,o=xi(this),a=In(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=QE(e)&&Mc(e);u&&this.injectableDefInScope(u)?l=Rs(Af(e),fc):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?ko():this.parent;return t=r&8&&t===Vr?null:t,c.get(e,t)}catch(c){let l=BE(c);throw l===-200||l===-201?new Xe(l,null):c}finally{In(a),xi(o)}}resolveInjectorInitializers(){let e=De(null),t=xi(this),i=In(void 0),r;try{let s=this.get(Ls,ti,{self:!0});for(let o of s)o()}finally{xi(t),In(i),De(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=An(e);let t=mc(e)?e:An(e&&e.provide),i=ZE(e);if(!mc(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Rs(void 0,fc,!0),r.factory=()=>If(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=De(null);try{if(t.value===zg)throw zf("");return t.value===fc&&(t.value=zg,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&KE(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{De(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=An(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Af(n){let e=Mc(n),t=e!==null?e.factory:Ns(n);if(t!==null)return t;if(n instanceof st)throw new Xe(-204,!1);if(n instanceof Function)return YE(n);throw new Xe(-204,!1)}function YE(n){if(n.length>0)throw new Xe(-204,!1);let t=FE(n);return t!==null?()=>t.factory(n):()=>new n}function ZE(n){if(n0(n))return Rs(void 0,n.useValue);{let e=i0(n);return Rs(e,fc)}}function i0(n,e,t){let i;if(mc(n)){let r=An(n);return Ns(r)||Af(r)}else if(n0(n))i=()=>An(n.useValue);else if(XE(n))i=()=>n.useFactory(...If(n.deps||[]));else if(qE(n))i=(r,s)=>gt(An(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=An(n&&(n.useClass||n.provide));if(JE(n))i=()=>new r(...If(n.deps));else return Ns(r)||Af(r)}return i}function Ao(n){if(n.destroyed)throw new Xe(-205,!1)}function Rs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function JE(n){return!!n.deps}function KE(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function QE(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Df(n,e){for(let t of n)Array.isArray(t)?Df(t,e):t&&Ff(t)?Df(t.\u0275providers,e):e(t)}function Ac(n,e){let t;n instanceof zr?(Ao(n),t=n):t=new Cf(n);let i,r=xi(t),s=In(void 0);try{return e()}finally{xi(r),In(s)}}function r0(){return Yg()!==void 0||dc()!=null}var ni=0,Le=1,Fe=2,$t=3,Gn=4,Wn=5,Os=6,Fs=7,zt=8,Hi=9,ii=10,Nt=11,ks=12,Zf=13,qr=14,jn=15,dr=16,Xr=17,bi=18,zi=19,Jf=20,Vi=21,Dc=22,cr=23,Dn=24,Yr=25,fr=26,Kt=27,s0=1,Kf=6,hr=7,Uo=8,Zr=9,Pt=10;function pr(n){return Array.isArray(n)&&typeof n[s0]=="object"}function ri(n){return Array.isArray(n)&&n[s0]===!0}function Qf(n){return(n.flags&4)!==0}function Jr(n){return n.componentOffset>-1}function Rc(n){return(n.flags&1)===1}function Kr(n){return!!n.template}function Us(n){return(n[Fe]&512)!==0}function Qr(n){return(n[Fe]&256)===256}var eh="svg",o0="math";function $n(n){for(;Array.isArray(n);)n=n[ni];return n}function th(n,e){return $n(e[n])}function si(n,e){return $n(e[n.index])}function Nc(n,e){return n.data[e]}function Gi(n,e){let t=e[n];return pr(t)?t:t[ni]}function Pc(n){return(n[Fe]&128)===128}function a0(n){return ri(n[$t])}function Mi(n,e){return e==null?null:n[e]}function nh(n){n[Xr]=0}function ih(n){n[Fe]&1024||(n[Fe]|=1024,Pc(n)&&es(n))}function c0(n,e){for(;n>0;)e=e[qr],n--;return e}function Bo(n){return!!(n[Fe]&9216||n[Dn]?.dirty)}function Lc(n){n[ii].changeDetectionScheduler?.notify(8),n[Fe]&64&&(n[Fe]|=1024),Bo(n)&&es(n)}function es(n){n[ii].changeDetectionScheduler?.notify(0);let e=lr(n);for(;e!==null&&!(e[Fe]&8192||(e[Fe]|=8192,!Pc(e)));)e=lr(e)}function rh(n,e){if(Qr(n))throw new Xe(911,!1);n[Vi]===null&&(n[Vi]=[]),n[Vi].push(e)}function l0(n,e){if(n[Vi]===null)return;let t=n[Vi].indexOf(e);t!==-1&&n[Vi].splice(t,1)}function lr(n){let e=n[$t];return ri(e)?e[$t]:e}function sh(n){return n[Fs]??=[]}function oh(n){return n.cleanup??=[]}function u0(n,e,t,i){let r=sh(e);r.push(t),n.firstCreatePass&&oh(n).push(i,r.length-1)}var $e={lFrame:T0(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Rf=!1;function d0(){return $e.lFrame.elementDepthCount}function f0(){$e.lFrame.elementDepthCount++}function ah(){$e.lFrame.elementDepthCount--}function h0(){return $e.bindingsEnabled}function p0(){return $e.skipHydrationRootTNode!==null}function ch(n){return $e.skipHydrationRootTNode===n}function lh(){$e.skipHydrationRootTNode=null}function tt(){return $e.lFrame.lView}function bn(){return $e.lFrame.tView}function sn(n){return $e.lFrame.contextLView=n,n[zt]}function on(n){return $e.lFrame.contextLView=null,n}function qn(){let n=uh();for(;n!==null&&n.type===64;)n=n.parent;return n}function uh(){return $e.lFrame.currentTNode}function m0(){let n=$e.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Bs(n,e){let t=$e.lFrame;t.currentTNode=n,t.isParent=e}function dh(){return $e.lFrame.isParent}function g0(){$e.lFrame.isParent=!1}function fh(){return Rf}function Ro(n){let e=Rf;return Rf=n,e}function v0(){return $e.lFrame.bindingIndex}function y0(n){return $e.lFrame.bindingIndex=n}function Vs(){return $e.lFrame.bindingIndex++}function Oc(n){let e=$e.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function _0(){return $e.lFrame.inI18n}function x0(n,e){let t=$e.lFrame;t.bindingIndex=t.bindingRootIndex=n,Fc(e)}function E0(){return $e.lFrame.currentDirectiveIndex}function Fc(n){$e.lFrame.currentDirectiveIndex=n}function b0(n){let e=$e.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function M0(){return $e.lFrame.currentQueryIndex}function kc(n){$e.lFrame.currentQueryIndex=n}function eb(n){let e=n[Le];return e.type===2?e.declTNode:e.type===1?n[Wn]:null}function hh(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=eb(s),r===null||(s=s[qr],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=$e.lFrame=S0();return i.currentTNode=e,i.lView=n,!0}function Uc(n){let e=S0(),t=n[Le];$e.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function S0(){let n=$e.lFrame,e=n===null?null:n.child;return e===null?T0(n):e}function T0(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function w0(){let n=$e.lFrame;return $e.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var ph=w0;function Bc(){let n=w0();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function C0(n){return($e.lFrame.contextLView=c0(n,$e.lFrame.contextLView))[zt]}function Wi(){return $e.lFrame.selectedIndex}function mr(n){$e.lFrame.selectedIndex=n}function mh(){let n=$e.lFrame;return Nc(n.tView,n.selectedIndex)}function Vc(){$e.lFrame.currentNamespace=eh}function I0(){return $e.lFrame.currentNamespace}var A0=!0;function Hc(){return A0}function zc(n){A0=n}function Nf(n,e=null,t=null,i){let r=D0(n,e,t,i);return r.resolveInjectorInitializers(),r}function D0(n,e=null,t=null,i,r=new Set){let s=[t||ti,e0(n)],o;return new zr(s,e||ko(),o||null,r)}var Ei=class n{static THROW_IF_NOT_FOUND=Vr;static NULL=new Do;static create(e,t){if(Array.isArray(e))return Nf({name:""},t,e,"");{let i=e.name??"";return Nf({name:i},e.parent,e.providers,i)}}static \u0275prov=Ht({token:n,providedIn:"any",factory:()=>gt($f)});static __NG_ELEMENT_ID__=-1},Xn=new st(""),It=(()=>{class n{static __NG_ELEMENT_ID__=tb;static __NG_ENV_ID__=t=>t}return n})(),gc=class extends It{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Qr(this._lView)}onDestroy(e){let t=this._lView;return rh(t,e),()=>l0(t,e)}};function tb(){return new gc(tt())}var R0=!1,N0=new st(""),Hs=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Io(!1);debugTaskTracker=Ie(N0,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new As(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Ht({token:n,providedIn:"root",factory:()=>new n})}return n})(),Pf=class extends Ui{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,r0()&&(this.destroyRef=Ie(It,{optional:!0})??void 0,this.pendingTasks=Ie(Hs,{optional:!0})??void 0)}emit(e){let t=De(null);try{super.next(e)}finally{De(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof pn&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Bi=Pf;function vc(...n){}function gh(n){let e,t;function i(){n=vc;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function P0(n){return queueMicrotask(()=>n()),()=>{n=vc}}var vh="isAngularZone",No=vh+"_ID",nb=0,mn=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Bi(!1);onMicrotaskEmpty=new Bi(!1);onStable=new Bi(!1);onError=new Bi(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=R0}=e;if(typeof Zone>"u")throw new Xe(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,sb(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(vh)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Xe(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Xe(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,ib,vc,vc);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},ib={};function yh(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function rb(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){gh(()=>{n.callbackScheduled=!1,Lf(n),n.isCheckStableRunning=!0,yh(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Lf(n)}function sb(n){let e=()=>{rb(n)},t=nb++;n._inner=n._inner.fork({name:"angular",properties:{[vh]:!0,[No]:t,[No+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(ob(c))return i.invokeTask(s,o,a,c);try{return Gg(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Wg(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return Gg(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!ab(c)&&e(),Wg(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Lf(n),yh(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Lf(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Gg(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Wg(n){n._nesting--,yh(n)}var Po=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Bi;onMicrotaskEmpty=new Bi;onStable=new Bi;onError=new Bi;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function ob(n){return L0(n,"__ignore_ng_zone__")}function ab(n){return L0(n,"__scheduler_tick__")}function L0(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var zn=class{_console=console;handleError(e){this._console.error("ERROR",e)}},ts=new st("",{factory:()=>{let n=Ie(mn),e=Ie(Hn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(zn),t.handleError(i))})}}}),O0={provide:Ls,useValue:()=>{let n=Ie(zn,{optional:!0})},multi:!0},cb=new st("",{factory:()=>{let n=Ie(Xn).defaultView;if(!n)return;let e=Ie(ts),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),Ie(It).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function _h(){return Ic([Qg(()=>{Ie(cb)})])}function Yt(n,e){let[t,i,r]=df(n,e?.equal),s=t,o=s[rn];return s.set=i,s.update=r,s.asReadonly=xh.bind(s),s}function xh(){let n=this[rn];if(n.readonlyFn===void 0){let e=()=>this();e[rn]=n,n.readonlyFn=e}return n.readonlyFn}var Vo=(()=>{class n{view;node;constructor(t,i){this.view=t,this.node=i}static __NG_ELEMENT_ID__=lb}return n})();function lb(){return new Vo(tt(),qn())}var ur=class{},Ho=new st("",{factory:()=>!0});var Eh=new st("");var Gc=(()=>{class n{static \u0275prov=Ht({token:n,providedIn:"root",factory:()=>new Of})}return n})(),Of=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},yc=class{[rn];constructor(e){this[rn]=e}destroy(){this[rn].destroy()}};function Wc(n,e){let t=e?.injector??Ie(Ei),i=e?.manualCleanup!==!0?t.get(It):null,r,s=t.get(Vo,null,{optional:!0}),o=t.get(ur);return s!==null?(r=fb(s.view,o,n),i instanceof gc&&i._lView===s.view&&(i=null)):r=hb(n,t.get(Gc),o),r.injector=t,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new yc(r)}var F0=Vt(St({},ff),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=Ro(!1);try{hf(this)}finally{Ro(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=De(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],De(n)}}}),ub=Vt(St({},F0),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(kr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),db=Vt(St({},F0),{consumerMarkedDirty(){this.view[Fe]|=8192,es(this.view),this.notifier.notify(13)},destroy(){if(kr(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[cr]?.delete(this)}});function fb(n,e,t){let i=Object.create(db);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=e,i.fn=k0(i,t),n[cr]??=new Set,n[cr].add(i),i.consumerMarkedDirty(i),i}function hb(n,e,t){let i=Object.create(ub);return i.fn=k0(i,n),i.scheduler=e,i.notifier=t,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function k0(n,e){return()=>{e(t=>(n.cleanupFns??=[]).push(t))}}function gv(n){return{toString:n}.toString()}function Sb(n){return typeof n=="function"}function vv(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Jc=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function Tb(n){return n.type.prototype.ngOnChanges&&(n.setInput=Cb),wb}function wb(){let n=_v(this),e=n?.current;if(e){let t=n.previous;if(t===$r)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function Cb(n,e,t,i,r){let s=this.declaredInputs[i],o=_v(n)||Ib(n,{previous:$r,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Jc(l&&l.currentValue,t,c===$r),vv(n,e,r,t)}var yv="__ngSimpleChanges__";function _v(n){return n[yv]||null}function Ib(n,e){return n[yv]=e}var U0=[];var ot=function(n,e=null,t){for(let i=0;i<U0.length;i++){let r=U0[i];r(n,e,t)}},nt=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(nt||{});function Ab(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Tb(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function Db(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function $c(n,e,t){xv(n,e,3,t)}function qc(n,e,t,i){(n[Fe]&3)===t&&xv(n,e,t,i)}function bh(n,e){let t=n[Fe];(t&3)===e&&(t&=16383,t+=1,n[Fe]=t)}function xv(n,e,t,i){let r=i!==void 0?n[Xr]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Xr]+=65536),(a<s||s==-1)&&(Rb(n,t,e,c),n[Xr]=(n[Xr]&4294901760)+c+2),c++}function B0(n,e){ot(nt.LifecycleHookStart,n,e);let t=De(null);try{e.call(n)}finally{De(t),ot(nt.LifecycleHookEnd,n,e)}}function Rb(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Fe]>>14<n[Xr]>>16&&(n[Fe]&3)===e&&(n[Fe]+=16384,B0(a,s)):B0(a,s)}var Gs=-1,jo=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function Nb(n){return(n.flags&8)!==0}function Pb(n){return(n.flags&16)!==0}function Lb(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];Fb(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function Ob(n){return n===3||n===4||n===6}function Fb(n){return n.charCodeAt(0)===64}function hl(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?V0(n,t,r,null,e[++i]):V0(n,t,r,null,null))}}return n}function V0(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Ev(n){return n!==Gs}function Kc(n){return n&32767}function kb(n){return n>>16}function Qc(n,e){let t=kb(n),i=e;for(;t>0;)i=i[qr],t--;return i}var Ah=!0;function H0(n){let e=Ah;return Ah=n,e}var Ub=256,bv=Ub-1,Mv=5,Bb=0,Si={};function Vb(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Gr)&&(i=t[Gr]),i==null&&(i=t[Gr]=Bb++);let r=i&bv,s=1<<r;e.data[n+(r>>Mv)]|=s}function Sv(n,e){let t=Tv(n,e);if(t!==-1)return t;let i=e[Le];i.firstCreatePass&&(n.injectorIndex=e.length,Mh(i.data,n),Mh(e,null),Mh(i.blueprint,null));let r=ip(n,e),s=n.injectorIndex;if(Ev(r)){let o=Kc(r),a=Qc(r,e),c=a[Le].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Mh(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Tv(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function ip(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Dv(r),i===null)return Gs;if(t++,r=r[qr],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Gs}function Hb(n,e,t){Vb(n,e,t)}function wv(n,e,t){if(t&8||n!==void 0)return n;Tc(e,"NodeInjector")}function Cv(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Hi],s=In(void 0);try{return r?r.get(e,i,t&8):Gf(e,i,t&8)}finally{In(s)}}return wv(i,e,t)}function Iv(n,e,t,i=0,r){if(n!==null){if(e[Fe]&2048&&!(i&2)){let o=jb(n,e,t,i,Si);if(o!==Si)return o}let s=Av(n,e,t,i,Si);if(s!==Si)return s}return Cv(e,t,i,r)}function Av(n,e,t,i,r){let s=Gb(t);if(typeof s=="function"){if(!hh(e,n,i))return i&1?wv(r,t,i):Cv(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Tc(t);else return o}finally{ph()}}else if(typeof s=="number"){let o=null,a=Tv(n,e),c=Gs,l=i&1?e[jn][Wn]:null;for((a===-1||i&4)&&(c=a===-1?ip(n,e):e[a+8],c===Gs||!G0(i,!1)?a=-1:(o=e[Le],a=Kc(c),e=Qc(c,e)));a!==-1;){let u=e[Le];if(z0(s,a,u.data)){let f=zb(a,e,t,o,i,l);if(f!==Si)return f}c=e[a+8],c!==Gs&&G0(i,e[Le].data[a+8]===l)&&z0(s,a,e)?(o=u,a=Kc(c),e=Qc(c,e)):a=-1}}return r}function zb(n,e,t,i,r,s){let o=e[Le],a=o.data[n+8],c=i==null?Jr(a)&&Ah:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Xc(a,o,t,c,l);return u!==null?el(e,o,u,a,r):Si}function Xc(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,f=i?a:a+u,d=r?a+u:l;for(let h=f;h<d;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&Kr(h)&&h.type===t)return c}return null}function el(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof jo){let a=s;if(a.resolving)throw zf("");let c=H0(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,f=a.injectImpl?In(a.injectImpl):null,d=hh(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&Ab(t,o[t],e)}finally{f!==null&&In(f),H0(c),a.resolving=!1,ph()}}return s}function Gb(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Gr)?n[Gr]:void 0;return typeof e=="number"?e>=0?e&bv:Wb:e}function z0(n,e,t){let i=1<<n;return!!(t[e+(n>>Mv)]&i)}function G0(n,e){return!(n&2)&&!(n&1&&e)}var ns=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Iv(this._tNode,this._lView,e,Hr(i),t)}};function Wb(){return new ns(qn(),tt())}function jb(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Fe]&2048&&!Us(o);){let a=Av(s,o,t,i|2,Si);if(a!==Si)return a;let c=s.parent;if(!c){let l=o[Jf];if(l){let u=l.get(t,Si,i&-5);if(u!==Si)return u}c=Dv(o),o=o[qr]}s=c}return r}function Dv(n){let e=n[Le],t=e.type;return t===2?e.declTNode:t===1?n[Wn]:null}function $b(){return qs(qn(),tt())}function qs(n,e){return new Qo(si(n,e))}var Qo=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=$b}return n})();function qb(n){return n instanceof Qo?n.nativeElement:n}function Xb(){return this._results[Symbol.iterator]()}var tl=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Ui}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Jg(e);(this._changesDetected=!Zg(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Xb};function Rv(n){return(n.flags&128)===128}var rp=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(rp||{}),Nv=new Map,Yb=0;function Zb(){return Yb++}function Jb(n){Nv.set(n[zi],n)}function Dh(n){Nv.delete(n[zi])}var W0="__ngContext__";function Ws(n,e){pr(e)?(n[W0]=e[zi],Jb(e)):n[W0]=e}function Pv(n){return Ov(n[ks])}function Lv(n){return Ov(n[Gn])}function Ov(n){for(;n!==null&&!ri(n);)n=n[Gn];return n}var Kb;function sp(n){Kb=n}var pl=new st("",{factory:()=>Qb}),Qb="ng";var ml=new st(""),ea=new st("",{providedIn:"platform",factory:()=>"unknown"});var gl=new st("",{factory:()=>Ie(Xn).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Fv="r";var kv="di";var Uv=!1,Bv=new st("",{factory:()=>Uv});var j0=new WeakMap;function eM(n,e){if(n==null||typeof n!="object")return;let t=j0.get(n);t||(t=new WeakSet,j0.set(n,t)),t.add(e)}var tM=(n,e,t,i)=>{};function nM(n,e,t,i){tM(n,e,t,i)}function op(n){return(n.flags&32)===32}var iM=()=>null;function Vv(n,e,t=!1){return iM(n,e,t)}function Hv(n,e){let t=n.contentQueries;if(t!==null){let i=De(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];kc(s),a.contentQueries(2,e[o],o)}}}finally{De(i)}}}function Rh(n,e,t){kc(0);let i=De(null);try{e(n,t)}finally{De(i)}}function zv(n,e,t){if(Qf(e)){let i=De(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{De(i)}}}var ai=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(ai||{});var nl=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${_c})`}};function ta(n){return n instanceof nl?n.changingThisBreaksApplicationSecurity:n}function Gv(n,e){let t=Wv(n);if(t!=null&&t!==e){if(t==="ResourceURL"&&e==="URL")return!0;throw new Error(`Required a safe ${e}, got a ${t} (see ${_c})`)}return t===e}function Wv(n){return n instanceof nl&&n.getTypeName()||null}var rM=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function jv(n){return n=String(n),n.match(rM)?n:"unsafe:"+n}function sM(n,e){return n.createText(e)}function oM(n,e,t){n.setValue(e,t)}function $v(n,e,t){return n.createElement(e,t)}function il(n,e,t,i,r){n.insertBefore(e,t,i,r)}function qv(n,e,t){n.appendChild(e,t)}function $0(n,e,t,i,r){i!==null?il(n,e,t,i,r):qv(n,e,t)}function Xv(n,e,t,i){n.removeChild(null,e,t,i)}function aM(n,e,t){n.setAttribute(e,"style",t)}function cM(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Yv(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&Lb(n,e,i),r!==null&&cM(n,e,r),s!==null&&aM(n,e,s)}var ap=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n})(ap||{});function Xs(n){let e=lM();return e?e.sanitize(ap.URL,n)||"":Gv(n,"URL")?ta(n):jv(jr(n))}function lM(){let n=tt();return n&&n[ii].sanitizer}function na(n){return n.ownerDocument.defaultView}function cp(n){return n.ownerDocument}function uM(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var Zv="ng-template";function dM(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&uM(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(lp(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function lp(n){return n.type===4&&n.value!==Zv}function fM(n,e,t){let i=n.type===4&&!t?Zv:n.value;return e===i}function hM(n,e,t){let i=4,r=n.attrs,s=r!==null?gM(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!oi(i)&&!oi(c))return!1;if(o&&oi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!fM(n,c,t)||c===""&&e.length===1){if(oi(i))return!1;o=!0}}else if(i&8){if(r===null||!dM(n,r,c,t)){if(oi(i))return!1;o=!0}}else{let l=e[++a],u=pM(c,r,lp(n),t);if(u===-1){if(oi(i))return!1;o=!0;continue}if(l!==""){let f;if(u>s?f="":f=r[u+1].toLowerCase(),i&2&&l!==f){if(oi(i))return!1;o=!0}}}}return oi(i)||o}function oi(n){return(n&1)===0}function pM(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return vM(e,n)}function mM(n,e,t=!1){for(let i=0;i<e.length;i++)if(hM(n,e[i],t))return!0;return!1}function gM(n){for(let e=0;e<n.length;e++){let t=n[e];if(Ob(t))return e}return n.length}function vM(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function q0(n,e){return n?":not("+e.trim()+")":e}function yM(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!oi(o)&&(e+=q0(s,r),r=""),i=o,s=s||!oi(i);t++}return r!==""&&(e+=q0(s,r)),e}function _M(n){return n.map(yM).join(",")}function xM(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!oi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Yn={};function up(n,e,t,i,r,s,o,a,c,l,u){let f=Kt+i,d=f+r,h=EM(f,d),g=typeof l=="function"?l():l;return h[Le]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:d,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function EM(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Yn);return t}function bM(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=up(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function dp(n,e,t,i,r,s,o,a,c,l,u){let f=e.blueprint.slice();return f[ni]=r,f[Fe]=i|4|128|8|64|1024,(l!==null||n&&n[Fe]&2048)&&(f[Fe]|=2048),nh(f),f[$t]=f[qr]=n,f[zt]=t,f[ii]=o||n&&n[ii],f[Nt]=a||n&&n[Nt],f[Hi]=c||n&&n[Hi]||null,f[Wn]=s,f[zi]=Zb(),f[Os]=u,f[Jf]=l,f[jn]=e.type==2?n[jn]:f,f}function MM(n,e,t){let i=si(e,n),r=bM(t),s=n[ii].rendererFactory,o=fp(n,dp(n,r,null,Jv(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function Jv(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Kv(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function fp(n,e){return n[ks]?n[Zf][Gn]=e:n[ks]=e,n[Zf]=e,e}function oe(n=1){Qv(bn(),tt(),Wi()+n,!1)}function Qv(n,e,t,i){if(!i)if((e[Fe]&3)===3){let s=n.preOrderCheckHooks;s!==null&&$c(e,s,t)}else{let s=n.preOrderHooks;s!==null&&qc(e,s,0,t)}mr(t)}var vl=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(vl||{});function Nh(n,e,t,i){let r=De(null);try{let[s,o,a]=n.inputs[t],c=null;(o&vl.SignalBased)!==0&&(c=e[s][rn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):vv(e,c,s,i)}finally{De(r)}}var ji=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(ji||{}),SM;function hp(n,e){return SM(n,e)}var aL=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Ph=new WeakMap,zo=new WeakSet;function TM(n,e){let t=Ph.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),zo.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function wM(n,e){let t=Ph.get(n);t?t.includes(e)||t.push(e):Ph.set(n,[e])}var is=new Set,yl=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(yl||{}),vr=new st(""),X0=new Set;function os(n){X0.has(n)||(X0.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var pp=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Ht({token:n,providedIn:"root",factory:()=>new n})}return n})(),ey=[0,1,2,3],ty=(()=>{class n{ngZone=Ie(mn);scheduler=Ie(ur);errorHandler=Ie(zn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){Ie(vr,{optional:!0})}execute(){let t=this.sequences.size>0;t&&ot(nt.AfterRenderHooksStart),this.executing=!0;for(let i of ey)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let s=r.hooks[i];return s(r.pipelinedValue)},r.snapshot))}catch(s){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(s)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&ot(nt.AfterRenderHooksEnd)}register(t){let{view:i}=t;i!==void 0?((i[Yr]??=[]).push(t),es(i),i[Fe]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,i){return i?i.run(yl.AFTER_NEXT_RENDER,t):t()}static \u0275prov=Ht({token:n,providedIn:"root",factory:()=>new n})}return n})(),rl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(e,t,i,r,s,o=null){this.impl=e,this.hooks=t,this.view=i,this.once=r,this.snapshot=o,this.unregisterOnDestroy=s?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let e=this.view?.[Yr];e&&(this.view[Yr]=e.filter(t=>t!==this))}};function Qt(n,e){let t=e?.injector??Ie(Ei);return os("NgAfterNextRender"),IM(n,t,e,!0)}function CM(n){return n instanceof Function?[void 0,void 0,n,void 0]:[n.earlyRead,n.write,n.mixedReadWrite,n.read]}function IM(n,e,t,i){let r=e.get(pp);r.impl??=e.get(ty);let s=e.get(vr,null,{optional:!0}),o=t?.manualCleanup!==!0?e.get(It):null,a=e.get(Vo,null,{optional:!0}),c=new rl(r.impl,CM(n),a?.view,i,o,s?.snapshot(null));return r.impl.register(c),c}var ny=new st("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:Ie(Hn)})});function iy(n,e,t){let i=n.get(ny);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function AM(n,e){let t=n.get(ny);if(e.detachedLeaveAnimationFns){for(let i of e.detachedLeaveAnimationFns)t.queue.delete(i);e.detachedLeaveAnimationFns=void 0}}function DM(n,e){for(let[t,i]of e)iy(n,i.animateFns)}function Y0(n,e,t,i){let r=n?.[fr]?.enter;e!==null&&r&&r.has(t.index)&&DM(i,r)}function zs(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;ri(r)?c=r:pr(r)&&(l=!0,r=r[ni]);let u=$n(r);n===0&&i!==null?(Y0(a,i,s,t),o==null?qv(e,i,u):il(e,i,u,o||null,!0)):n===1&&i!==null?(Y0(a,i,s,t),il(e,i,u,o||null,!0),TM(s,u)):n===2?(a?.[fr]?.leave?.has(s.index)&&wM(s,u),zo.delete(u),Z0(a,s,t,f=>{if(zo.has(u)){zo.delete(u);return}Xv(e,u,l,f)})):n===3&&(zo.delete(u),Z0(a,s,t,()=>{e.destroyNode(u)})),c!=null&&GM(e,n,t,c,s,i,o)}}function RM(n,e){ry(n,e),e[ni]=null,e[Wn]=null}function NM(n,e,t,i,r,s){i[ni]=r,i[Wn]=e,xl(n,i,t,1,r,s)}function ry(n,e){e[ii].changeDetectionScheduler?.notify(9),xl(n,e,e[Nt],2,null,null)}function PM(n){let e=n[ks];if(!e)return Sh(n[Le],n);for(;e;){let t=null;if(pr(e))t=e[ks];else{let i=e[Pt];i&&(t=i)}if(!t){for(;e&&!e[Gn]&&e!==n;)pr(e)&&Sh(e[Le],e),e=e[$t];e===null&&(e=n),pr(e)&&Sh(e[Le],e),t=e&&e[Gn]}e=t}}function mp(n,e){let t=n[Zr],i=t.indexOf(e);t.splice(i,1)}function _l(n,e){if(Qr(e))return;let t=e[Nt];t.destroyNode&&xl(n,e,t,3,null,null),PM(e)}function Sh(n,e){if(Qr(e))return;let t=De(null);try{e[Fe]&=-129,e[Fe]|=256,e[Dn]&&kr(e[Dn]),FM(n,e),OM(n,e),e[Le].type===1&&e[Nt].destroy();let i=e[dr];if(i!==null&&ri(e[$t])){i!==e[$t]&&mp(i,e);let r=e[bi];r!==null&&r.detachView(n)}Dh(e)}finally{De(t)}}function Z0(n,e,t,i){let r=n?.[fr];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&is.add(n[zi]),iy(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),LM(n,i)}else n&&is.delete(n[zi]),i(!1)},r)}function LM(n,e){let t=n[fr]?.running;if(t){t.then(()=>{n[fr].running=void 0,is.delete(n[zi]),e(!0)});return}e(!1)}function OM(n,e){let t=n.cleanup,i=e[Fs];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Fs]=null);let r=e[Vi];if(r!==null){e[Vi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[cr];if(s!==null){e[cr]=null;for(let o of s)o.destroy()}}function FM(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof jo)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];ot(nt.LifecycleHookStart,a,c);try{c.call(a)}finally{ot(nt.LifecycleHookEnd,a,c)}}else{ot(nt.LifecycleHookStart,r,s);try{s.call(r)}finally{ot(nt.LifecycleHookEnd,r,s)}}}}}function kM(n,e,t){return UM(n,e.parent,t)}function UM(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[ni];if(Jr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ai.None||r===ai.Emulated)return null}return si(i,t)}function BM(n,e,t){return HM(n,e,t)}function VM(n,e,t){return n.type&40?si(n,t):null}var HM=VM,J0;function gp(n,e,t,i){let r=kM(n,i,e),s=e[Nt],o=i.parent||e[Wn],a=BM(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)$0(s,r,t[c],a,!1);else $0(s,r,t,a,!1);J0!==void 0&&J0(s,i,e,t,r)}function Go(n,e){if(e!==null){let t=e.type;if(t&3)return si(e,n);if(t&4)return Lh(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Go(n,i);{let r=n[e.index];return ri(r)?Lh(-1,r):$n(r)}}else{if(t&128)return Go(n,e.next);if(t&32)return hp(e,n)()||$n(n[e.index]);{let i=sy(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=lr(n[jn]);return Go(r,i)}else return Go(n,e.next)}}}return null}function sy(n,e){if(e!==null){let i=n[jn][Wn],r=e.projection;return i.projection[r]}return null}function Lh(n,e){let t=Pt+n+1;if(t<e.length){let i=e[t],r=i[Le].firstChild;if(r!==null)return Go(i,r)}return e[hr]}function vp(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Hi];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Ws($n(c),i),t.flags|=2),!op(t))if(l&8)vp(n,e,t.child,i,r,s,!1),zs(e,n,a,r,c,t,s,i);else if(l&32){let u=hp(t,i),f;for(;f=u();)zs(e,n,a,r,f,t,s,i);zs(e,n,a,r,c,t,s,i)}else l&16?zM(n,e,i,t,r,s):zs(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function xl(n,e,t,i,r,s){vp(t,i,n.firstChild,e,r,s,!1)}function zM(n,e,t,i,r,s){let o=t[jn],c=o[Wn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];zs(e,n,t[Hi],r,u,i,s,t)}else{let l=c,u=o[$t];Rv(i)&&(l.flags|=128),vp(n,e,l,u,r,s,!0)}}function GM(n,e,t,i,r,s,o){let a=i[hr],c=$n(i);a!==c&&zs(e,n,t,s,a,r,o);for(let l=Pt;l<i.length;l++){let u=i[l];xl(u[Le],u,n,e,s,a)}}function WM(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:ji.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=ji.Important),n.setStyle(t,i,r,s))}}function oy(n,e,t,i,r){let s=Wi(),o=i&2;try{mr(-1),o&&e.length>Kt&&Qv(n,e,Kt,!1);let a=o?nt.TemplateUpdateStart:nt.TemplateCreateStart;ot(a,r,t),t(i,r)}finally{mr(s);let a=o?nt.TemplateUpdateEnd:nt.TemplateCreateEnd;ot(a,r,t)}}function ay(n,e,t){YM(n,e,t),(t.flags&64)===64&&ZM(n,e,t)}function yp(n,e,t=si){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function jM(n,e,t,i){let s=i.get(Bv,Uv)||t===ai.ShadowDom||t===ai.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return $M(o),o}function $M(n){qM(n)}var qM=()=>null;function XM(n,e,t,i,r,s){if(n.type&3){let o=si(n,e);i=s!=null?s(i,n.value||"",t):i,r.setProperty(o,t,i)}else n.type&12}function YM(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Jr(t)&&MM(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Sv(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=el(e,n,o,t);if(Ws(c,e),s!==null&&tS(e,o-i,c,a,t,s),Kr(a)){let l=Gi(t.index,e);l[zt]=el(e,n,o,t)}}}function ZM(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=E0();try{mr(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Fc(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&JM(c,l)}}finally{mr(-1),Fc(o)}}function JM(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function KM(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];mM(e,s.selectors,!1)&&(i??=[],Kr(s)?i.unshift(s):i.push(s))}return i}function QM(n,e,t,i,r,s){let o=si(n,e);eS(e[Nt],o,s,n.value,t,i,r)}function eS(n,e,t,i,r,s,o){if(s==null)n.removeAttribute(e,r,t);else{let a=o==null?jr(s):o(s,i||"",r);n.setAttribute(e,r,a,t)}}function tS(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Nh(i,t,c,l)}}function cy(n,e,t,i,r){let s=Kt+t,o=e[Le],a=r(o,e,n,i,t);e[s]=a,Bs(n,!0);let c=n.type===2;return c?(Yv(e[Nt],a,n),(d0()===0||Rc(n))&&Ws(a,e),f0()):Ws(a,e),Hc()&&(!c||!op(n))&&gp(o,e,a,n),n}function ly(n){let e=n;return dh()?g0():(e=e.parent,Bs(e,!1)),e}function nS(n,e){let t=n[Hi];if(!t)return;let i;try{i=t.get(ts,null)}catch{i=null}i?.(e)}function uy(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],f=e.data[l];Nh(f,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Nh(u,l,i,r),a=!0}return a}function iS(n,e){let t=Gi(e,n),i=t[Le];rS(i,t);let r=t[ni];r!==null&&t[Os]===null&&(t[Os]=Vv(r,t[Hi])),ot(nt.ComponentStart);try{_p(i,t,t[zt])}finally{ot(nt.ComponentEnd,t[zt])}}function rS(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function _p(n,e,t){Uc(e);try{let i=n.viewQuery;i!==null&&Rh(1,i,t);let r=n.template;r!==null&&oy(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[bi]?.finishViewCreation(n),n.staticContentQueries&&Hv(n,e),n.staticViewQueries&&Rh(2,n.viewQuery,t);let s=n.components;s!==null&&sS(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Fe]&=-5,Bc()}}function sS(n,e){for(let t=0;t<e.length;t++)iS(n,e[t])}function El(n,e,t,i){let r=De(null);try{let s=e.tView,a=n[Fe]&4096?4096:16,c=dp(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[dr]=l;let u=n[bi];return u!==null&&(c[bi]=u.createEmbeddedView(s)),_p(s,c,t),c}finally{De(r)}}function $o(n,e){return!e||e.firstChild===null||Rv(n)}function qo(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push($n(s)),ri(s)&&dy(s,i);let o=t.type;if(o&8)qo(n,e,t.child,i);else if(o&32){let a=hp(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=sy(e,t);if(Array.isArray(a))i.push(...a);else{let c=lr(e[jn]);qo(c[Le],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function dy(n,e){for(let t=Pt;t<n.length;t++){let i=n[t],r=i[Le].firstChild;r!==null&&qo(i[Le],i,r,e)}n[hr]!==n[ni]&&e.push(n[hr])}function fy(n){if(n[Yr]!==null){for(let e of n[Yr])e.impl.addSequence(e);n[Yr].length=0}}var hy=[];function oS(n){return n[Dn]??aS(n)}function aS(n){let e=hy.pop()??Object.create(lS);return e.lView=n,e}function cS(n){n.lView[Dn]!==n&&(n.lView=null,hy.push(n))}var lS=Vt(St({},Ms),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{es(n.lView)},consumerOnSignalRead(){this.lView[Dn]=this}});function uS(n){let e=n[Dn]??Object.create(dS);return e.lView=n,e}var dS=Vt(St({},Ms),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=lr(n.lView);for(;e&&!py(e[Le]);)e=lr(e);e&&ih(e)},consumerOnSignalRead(){this.lView[Dn]=this}});function py(n){return n.type!==2}function my(n){if(n[cr]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[cr])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Fe]&8192)}}var fS=100;function gy(n,e=0){let i=n[ii].rendererFactory,r=!1;r||i.begin?.();try{hS(n,e)}finally{r||i.end?.()}}function hS(n,e){let t=fh();try{Ro(!0),Oh(n,e);let i=0;for(;Bo(n);){if(i===fS)throw new Xe(103,!1);i++,Oh(n,1)}}finally{Ro(t)}}function pS(n,e,t,i){if(Qr(e))return;let r=e[Fe],s=!1,o=!1;Uc(e);let a=!0,c=null,l=null;s||(py(n)?(l=oS(e),c=Ss(l)):ic()===null?(a=!1,l=uS(e),c=Ss(l)):e[Dn]&&(kr(e[Dn]),e[Dn]=null));try{nh(e),y0(n.bindingStartIndex),t!==null&&oy(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&$c(e,h,null)}else{let h=n.preOrderHooks;h!==null&&qc(e,h,0,null),bh(e,0)}if(o||mS(e),my(e),vy(e,0),n.contentQueries!==null&&Hv(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&$c(e,h)}else{let h=n.contentHooks;h!==null&&qc(e,h,1),bh(e,1)}vS(n,e);let f=n.components;f!==null&&_y(e,f,0);let d=n.viewQuery;if(d!==null&&Rh(2,d,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&$c(e,h)}else{let h=n.viewHooks;h!==null&&qc(e,h,2),bh(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Dc]){for(let h of e[Dc])h();e[Dc]=null}s||(fy(e),e[Fe]&=-73)}catch(u){throw s||es(e),u}finally{l!==null&&(So(l,c),a&&cS(l)),Bc()}}function vy(n,e){for(let t=Pv(n);t!==null;t=Lv(t))for(let i=Pt;i<t.length;i++){let r=t[i];yy(r,e)}}function mS(n){for(let e=Pv(n);e!==null;e=Lv(e)){if(!(e[Fe]&2))continue;let t=e[Zr];for(let i=0;i<t.length;i++){let r=t[i];ih(r)}}}function gS(n,e,t){ot(nt.ComponentStart);let i=Gi(e,n);try{yy(i,t)}finally{ot(nt.ComponentEnd,i[zt])}}function yy(n,e){Pc(n)&&Oh(n,e)}function Oh(n,e){let i=n[Le],r=n[Fe],s=n[Dn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&To(s)),o||=!1,s&&(s.dirty=!1),n[Fe]&=-9217,o)pS(i,n,i.template,n[zt]);else if(r&8192){let a=De(null);try{my(n),vy(n,1);let c=i.components;c!==null&&_y(n,c,1),fy(n)}finally{De(a)}}}function _y(n,e,t){for(let i=0;i<e.length;i++)gS(n,e[i],t)}function vS(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)mr(~r);else{let s=r,o=t[++i],a=t[++i];x0(o,s);let c=e[s];ot(nt.HostBindingsUpdateStart,c);try{a(2,c)}finally{ot(nt.HostBindingsUpdateEnd,c)}}}}finally{mr(-1)}}function xp(n,e){let t=fh()?64:1088;for(n[ii].changeDetectionScheduler?.notify(e);n;){n[Fe]|=t;let i=lr(n);if(Us(n)&&!i)return n;n=i}return null}function xy(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Ey(n,e){let t=Pt+e;if(t<n.length)return n[t]}function bl(n,e,t,i=!0){let r=e[Le];if(yS(r,e,n,t),i){let o=Lh(t,n),a=e[Nt],c=a.parentNode(n[hr]);c!==null&&NM(r,n[Wn],a,e,c,o)}let s=e[Os];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function by(n,e){let t=Xo(n,e);return t!==void 0&&_l(t[Le],t),t}function Xo(n,e){if(n.length<=Pt)return;let t=Pt+e,i=n[t];if(i){let r=i[dr];r!==null&&r!==n&&mp(r,i),e>0&&(n[t-1][Gn]=i[Gn]);let s=Oo(n,Pt+e);RM(i[Le],i);let o=s[bi];o!==null&&o.detachView(s[Le]),i[$t]=null,i[Gn]=null,i[Fe]&=-129}return i}function yS(n,e,t,i){let r=Pt+i,s=t.length;i>0&&(t[r-1][Gn]=e),i<s-Pt?(e[Gn]=t[r],Wf(t,Pt+i,e)):(t.push(e),e[Gn]=null),e[$t]=t;let o=e[dr];o!==null&&t!==o&&My(o,e);let a=e[bi];a!==null&&a.insertView(n),Lc(e),e[Fe]|=128}function My(n,e){let t=n[Zr],i=e[$t];if(pr(i))n[Fe]|=2;else{let r=i[$t][jn];e[jn]!==r&&(n[Fe]|=2)}t===null?n[Zr]=[e]:t.push(e)}var js=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Le];return qo(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[zt]}set context(e){this._lView[zt]=e}get destroyed(){return Qr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[$t];if(ri(e)){let t=e[Uo],i=t?t.indexOf(this):-1;i>-1&&(Xo(e,i),Oo(t,i))}this._attachedToViewContainer=!1}_l(this._lView[Le],this._lView)}onDestroy(e){rh(this._lView,e)}markForCheck(){xp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Fe]&=-129}reattach(){Lc(this._lView),this._lView[Fe]|=128}detectChanges(){this._lView[Fe]|=1024,gy(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Xe(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Us(this._lView),t=this._lView[dr];t!==null&&!e&&mp(t,this._lView),ry(this._lView[Le],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Xe(902,!1);this._appRef=e;let t=Us(this._lView),i=this._lView[dr];i!==null&&!t&&My(i,this._lView),Lc(this._lView)}};var Yo=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=_S;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=El(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new js(s)}}return n})();function _S(){return Ep(qn(),tt())}function Ep(n,e){return n.type&4?new Yo(e,n,qs(n,e)):null}function Ml(n,e,t,i,r){let s=n.data[e];if(s===null)s=xS(n,e,t,i,r),_0()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=m0();s.injectorIndex=o===null?-1:o.injectorIndex}return Bs(s,!0),s}function xS(n,e,t,i,r){let s=uh(),o=dh(),a=o?s:s&&s.parent,c=n.data[e]=bS(n,a,t,e,i,r);return ES(n,c,s,o),c}function ES(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function bS(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return p0()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function MS(n){let e=n[Kf]??[],i=n[$t][Nt],r=[];for(let s of e)s.data[kv]!==void 0?r.push(s):SS(s,i);n[Kf]=r}function SS(n,e){let t=0,i=n.firstChild;if(i){let r=n.data[Fv];for(;t<r;){let s=i.nextSibling;Xv(e,i,!1),i=s,t++}}}var TS=()=>null,wS=()=>null;function Fh(n,e){return TS(n,e)}function Sy(n,e,t){return wS(n,e,t)}var Ty=class{},Sl=class{},kh=class{resolveComponentFactory(e){throw new Xe(917,!1)}},Tl=class{static NULL=new kh},rs=class{};var wy=(()=>{class n{static \u0275prov=Ht({token:n,providedIn:"root",factory:()=>null})}return n})();var Yc={},Uh=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Yc,i);return r!==Yc||t===Yc?r:this.parentInjector.get(e,t,i)}};function sl(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Ec(r,a);else if(s==2){let c=a,l=e[++o];i=Ec(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Cy(n,e=0){let t=tt();if(t===null)return gt(n,e);let i=qn();return Iv(i,t,An(n),e)}function CS(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}DS(n,e,t,a,s,c,l)}s!==null&&i!==null&&IS(t,i,s)}function IS(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Xe(-301,!1);i.push(e[r],s)}}function AS(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function DS(n,e,t,i,r,s,o){let a=i.length,c=null;for(let d=0;d<a;d++){let h=i[d];c===null&&Kr(h)&&(c=h,AS(n,t,d)),Hb(Sv(t,e),n,h.type)}FS(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let d=0;d<a;d++){let h=i[d];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,f=Kv(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let d=0;d<a;d++){let h=i[d];if(t.mergedAttrs=hl(t.mergedAttrs,h.hostAttrs),NS(n,t,e,f,h),OS(f,h,r),o!==null&&o.has(h)){let[x,m]=o.get(h);t.directiveToIndex.set(h.type,[f,x+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),f++}RS(n,t,s)}function RS(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))K0(0,e,r,i),K0(1,e,r,i),ev(e,i,!1);else{let s=t.get(r);Q0(0,e,s,i),Q0(1,e,s,i),ev(e,i,!0)}}}function K0(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),Iy(e,s)}}function Q0(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),Iy(e,o)}}function Iy(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function ev(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||lp(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function NS(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Ns(r.type,!0)),o=new jo(s,Kr(r),Cy,null);n.blueprint[i]=o,t[i]=o,PS(n,e,i,Kv(n,t,r.hostVars,Yn),r)}function PS(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;LS(o)!=a&&o.push(a),o.push(t,i,s)}}function LS(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function OS(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Kr(e)&&(t[""]=n)}}function FS(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function Ay(n,e,t,i,r,s,o,a){let c=e[Le],l=c.consts,u=Mi(l,o),f=Ml(c,n,t,i,u);return s&&CS(c,e,f,Mi(l,a),r),f.mergedAttrs=hl(f.mergedAttrs,f.attrs),f.attrs!==null&&sl(f,f.attrs,!1),f.mergedAttrs!==null&&sl(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Dy(n,e){Db(n,e),Qf(e)&&n.queries.elementEnd(e)}function kS(n,e,t,i,r,s){let o=e.consts,a=Mi(o,r),c=Ml(e,n,t,i,a);if(c.mergedAttrs=hl(c.mergedAttrs,c.attrs),s!=null){let l=Mi(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&sl(c,c.attrs,!1),c.mergedAttrs!==null&&sl(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function Ti(n,e,t){if(t===Yn)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function US(n,e,t,i){let r=Ti(n,e,t);return Ti(n,e+1,i)||r}function BS(n,e,t,i,r){let s=US(n,e,t,i);return Ti(n,e+2,r)||s}function Zc(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&eM(r,s);let o=Jr(n)?Gi(n.index,e):e;xp(o,5);let a=e[zt],c=tv(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=tv(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function tv(n,e,t,i){let r=De(null);try{return ot(nt.OutputStart,e,t),t(i)!==!1}catch(s){return nS(n,s),!1}finally{ot(nt.OutputEnd,e,t),De(r)}}function Ry(n,e,t,i,r,s,o,a){let c=Rc(n),l=!1,u=null;if(!i&&c&&(u=HS(e,t,s,n.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let f=si(n,t),d=i?i(f):f;nM(t,d,s,a),i||(a.__ngNativeEl__=f);let h=r.listen(d,s,a);if(!VS(s)){let g=i?x=>i($n(x[n.index])):n.index;Ny(g,e,t,s,a,h,!1)}}return l}function VS(n){return n.startsWith("animation")||n.startsWith("transition")}function HS(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Fs],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function Ny(n,e,t,i,r,s,o){let a=e.firstCreatePass?oh(e):null,c=sh(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}function nv(n,e,t,i,r,s){let o=e[t],a=e[Le],l=a.data[t].outputs[i],f=o[l].subscribe(s);Ny(n.index,a,e,r,s,f,!0)}var Bh=Symbol("BINDING");function Py(n){return n.debugInfo?.className||n.type.name||null}var Vh=class extends Tl{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Wr(e);return new Zo(t,this.ngModule)}};function zS(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&vl.SignalBased)!==0};return r&&(s.transform=r),s})}function GS(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function WS(n,e,t){let i=e instanceof Hn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Uh(t,i):t}function jS(n){let e=n.get(rs,null);if(e===null)throw new Xe(407,!1);let t=n.get(wy,null),i=n.get(ur,null),r=n.get(vr,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function $S(n,e){let t=Ly(n);return $v(e,t,t==="svg"?eh:t==="math"?o0:null)}function Ly(n){return(n.selectors[0][0]||"div").toLowerCase()}var Zo=class extends Sl{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=zS(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=GS(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=_M(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){ot(nt.DynamicComponentStart);let a=De(null);try{let c=this.componentDef,l=WS(c,r||this.ngModule,e),u=jS(l),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(Py(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{De(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=qS(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?jM(l,r,a.encapsulation,t):$S(a,l),f=o?.some(iv)||s?.some(g=>typeof g!="function"&&g.bindings.some(iv)),d=dp(null,c,null,512|Jv(a),null,null,e,l,t,null,Vv(u,t,!0));d[Kt]=u,Uc(d);let h=null;try{let g=Ay(Kt,d,2,"#host",()=>c.directiveRegistry,!0,0);Yv(l,u,g),Ws(u,d),ay(c,d,g),zv(c,g,d),Dy(c,g),i!==void 0&&YS(g,this.ngContentSelectors,i),h=Gi(g.index,d),d[zt]=h[zt],_p(c,d,null)}catch(g){throw h!==null&&Dh(h),Dh(d),g}finally{ot(nt.DynamicComponentEnd),Bc()}return new ol(this.componentType,d,!!f)}};function qS(n,e,t,i){let r=n?["ng-version","21.2.12"]:xM(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[Bh].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let d of f.bindings){a+=d[Bh].requiredVars;let h=u+1;d.create&&(d.targetIdx=h,(s??=[]).push(d)),d.update&&(d.targetIdx=h,(o??=[]).push(d))}}let c=[e];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,d=Vf(f);c.push(d)}return up(0,null,XS(s,o),1,a,c,null,null,null,[r],null)}function XS(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function iv(n){let e=n[Bh].kind;return e==="input"||e==="twoWay"}var ol=class extends Ty{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Nc(t[Le],Kt),this.location=qs(this._tNode,t),this.instance=Gi(this._tNode.index,t)[zt],this.hostView=this.changeDetectorRef=new js(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=uy(i,r[Le],r,e,t);this.previousInputValues.set(e,t);let o=Gi(i.index,r);xp(o,1)}get injector(){return new ns(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function YS(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var wl=(()=>{class n{static __NG_ELEMENT_ID__=ZS}return n})();function ZS(){let n=qn();return Oy(n,tt())}var Hh=class n extends wl{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return qs(this._hostTNode,this._hostLView)}get injector(){return new ns(this._hostTNode,this._hostLView)}get parentInjector(){let e=ip(this._hostTNode,this._hostLView);if(Ev(e)){let t=Qc(e,this._hostLView),i=Kc(e),r=t[Le].data[i+8];return new ns(r,t)}else return new ns(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=rv(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Pt}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Fh(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,$o(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!Sb(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new Zo(Wr(e)),f=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?f:this.parentInjector).get(Hn,null);p&&(s=p)}let d=Wr(u.componentType??{}),h=Fh(this._lContainer,d?.id??null),g=h?.firstChild??null,x=u.create(f,r,g,s,o,a);return this.insertImpl(x.hostView,l,$o(this._hostTNode,h)),x}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(a0(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[$t],l=new n(c,c[Wn],c[$t]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return bl(o,r,s,i),e.attachToViewContainerRef(),Wf(Th(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=rv(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Xo(this._lContainer,t);i&&(Oo(Th(this._lContainer),t),_l(i[Le],i))}detach(e){let t=this._adjustIndex(e,-1),i=Xo(this._lContainer,t);return i&&Oo(Th(this._lContainer),t)!=null?new js(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function rv(n){return n[Uo]}function Th(n){return n[Uo]||(n[Uo]=[])}function Oy(n,e){let t,i=e[n.index];return ri(i)?t=i:(t=xy(i,e,null,n),e[n.index]=t,fp(e,t)),KS(t,e,n,i),new Hh(t,n,e)}function JS(n,e){let t=n[Nt],i=t.createComment(""),r=si(e,n),s=t.parentNode(r);return il(t,s,i,t.nextSibling(r),!1),i}var KS=tT,QS=()=>!1;function eT(n,e,t){return QS(n,e,t)}function tT(n,e,t,i){if(n[hr])return;let r;t.type&8?r=$n(i):r=JS(e,t),n[hr]=r}var zh=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Gh=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Uy(e,t).matches!==null&&this.queries[t].setDirty()}},Wh=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=cT(e):this.predicate=e}},jh=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},$h=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,nT(t,s)),this.matchTNodeWithReadOption(e,t,Xc(t,e,s,!1,!1))}else i===Yo?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Xc(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Qo||r===wl||r===Yo&&t.type&4)this.addMatch(t.index,-2);else{let s=Xc(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function nT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function iT(n,e){return n.type&11?qs(n,e):n.type&4?Ep(n,e):null}function rT(n,e,t,i){return t===-1?iT(e,n):t===-2?sT(n,e,i):el(n,n[Le],t,e)}function sT(n,e,t){if(t===Qo)return qs(e,n);if(t===Yo)return Ep(e,n);if(t===wl)return Oy(e,n)}function Fy(n,e,t,i){let r=e[bi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(rT(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function qh(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=Fy(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let f=Pt;f<u.length;f++){let d=u[f];d[dr]===d[$t]&&qh(d[Le],d,l,i)}if(u[Zr]!==null){let f=u[Zr];for(let d=0;d<f.length;d++){let h=f[d];qh(h[Le],h,l,i)}}}}}return i}function ky(n,e){return n[bi].queries[e].queryList}function oT(n,e,t){let i=new tl((t&4)===4);return u0(n,e,i,i.destroy),(e[bi]??=new Gh).queries.push(new zh(i))-1}function aT(n,e,t){let i=bn();return i.firstCreatePass&&(lT(i,new Wh(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),oT(i,tt(),e)}function cT(n){return n.split(",").map(e=>e.trim())}function lT(n,e,t){n.queries===null&&(n.queries=new jh),n.queries.track(new $h(e,t))}function Uy(n,e){return n.queries.getByIndex(e)}function uT(n,e){let t=n[Le],i=Uy(t,e);return i.crossesNgTemplate?qh(t,n,e,[]):Fy(t,n,i,e)}function By(n,e,t){let i,r=rc(()=>{i._dirtyCounter();let s=fT(i,n);if(e&&s===void 0)throw new Xe(-951,!1);return s});return i=r[rn],i._dirtyCounter=Yt(0),i._flatValue=void 0,r}function Vy(n){return By(!0,!1,n)}function Hy(n){return By(!0,!0,n)}function dT(n,e){let t=n[rn];t._lView=tt(),t._queryIndex=e,t._queryList=ky(t._lView,e),t._queryList.onDirty(()=>t._dirtyCounter.update(i=>i+1))}function fT(n,e){let t=n._lView,i=n._queryIndex;if(t===void 0||i===void 0||t[Fe]&4)return e?void 0:ti;let r=ky(t,i),s=uT(t,i);return r.reset(s,qb),e?r.first:r._changesDetected||n._flatValue===void 0?n._flatValue=r.toArray():n._flatValue}var al=class{};var Jo=class extends al{injector;componentFactoryResolver=new Vh(this);instance=null;constructor(e){super();let t=new zr([...e.providers,{provide:al,useValue:this},{provide:Tl,useValue:this.componentFactoryResolver}],e.parent||ko(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function zy(n,e,t=null){return new Jo({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var hT=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Xf(!1,t.type),r=i.length>0?zy([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Ht({token:n,providedIn:"environment",factory:()=>new n(gt(Hn))})}return n})();function ct(n){return gv(()=>{let e=vT(n),t=Vt(St({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===rp.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(hT).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ai.Emulated,styles:n.styles||ti,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&os("NgStandalone"),yT(t);let i=n.dependencies;return t.directiveDefs=sv(i,pT),t.pipeDefs=sv(i,$g),t.id=_T(t),t})}function pT(n){return Wr(n)||Vf(n)}function mT(n,e){if(n==null)return $r;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=vl.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function gT(n){if(n==null)return $r;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function vT(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||$r,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||ti,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:mT(n.inputs,e),outputs:gT(n.outputs),debugInfo:null}}function yT(n){n.features?.forEach(e=>e(n))}function sv(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function _T(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function xT(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=hl(n.mergedAttrs,n.attrs);let u=n.tView=up(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Bs(n,!1);let c=ET(t,e,n,i);Hc()&&gp(t,e,c,n),Ws(c,e);let l=xy(c,e,c,n);e[i+Kt]=l,fp(e,l),eT(l,n,e)}function cl(n,e,t,i,r,s,o,a,c,l,u){let f=t+Kt,d;if(e.firstCreatePass){if(d=Ml(e,f,4,o||null,a||null),l!=null){let h=Mi(e.consts,l);d.localNames=[];for(let g=0;g<h.length;g+=2)d.localNames.push(h[g],-1)}}else d=e.data[f];return xT(d,n,e,t,i,r,s,c),l!=null&&yp(n,d,u),d}var ET=bT;function bT(n,e,t,i){return zc(!0),e[Nt].createComment("")}var bp=new st("");function Mp(n){return!!n&&typeof n.then=="function"}function Gy(n){return!!n&&typeof n.subscribe=="function"}var Wy=new st("");var Sp=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=Ie(Wy,{optional:!0})??[];injector=Ie(Ei);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Ac(this.injector,r);if(Mp(s))t.push(s);else if(Gy(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ht({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),jy=new st("");function $y(){uf(()=>{let n="";throw new Xe(600,n)})}function qy(n){return n.isBoundToModule}var MT=10;var Cl=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=Ie(ts);afterRenderManager=Ie(pp);zonelessEnabled=Ie(Ho);rootEffectScheduler=Ie(Gc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Ui;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=Ie(Hs);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Ef(t=>!t))}constructor(){Ie(vr,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=Ie(Hn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=Ei.NULL){return this._injector.get(mn).run(()=>{ot(nt.BootstrapComponentStart);let o=t instanceof Sl;if(!this._injector.get(Sp).done){let g="";throw new Xe(405,g)}let c;o?c=t:c=this._injector.get(Tl).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=qy(c)?void 0:this._injector.get(al),u=i||c.selector,f=c.create(r,[],u,l),d=f.location.nativeElement,h=f.injector.get(bp,null);return h?.registerApplication(d),f.onDestroy(()=>{this.detachView(f.hostView),Wo(this.components,f),h?.unregisterApplication(d)}),this._loadComponent(f),ot(nt.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ot(nt.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(yl.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ot(nt.ChangeDetectionEnd),new Xe(101,!1);let t=De(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,De(t),this.afterTick.next(),ot(nt.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(rs,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<MT;){ot(nt.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ot(nt.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Bo(r))continue;let s=i&&!this.zonelessEnabled?0:1;gy(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Bo(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Wo(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(jy,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Wo(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Xe(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ht({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Wo(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function ia(n,e,t,i){let r=tt(),s=Vs();if(Ti(r,s,e)){let o=bn(),a=mh();QM(a,r,n,e,t,i)}return ia}var Xh=class{destroy(e){}updateValue(e,t){}swap(e,t){let i=Math.min(e,t),r=Math.max(e,t),s=this.detach(r);if(r-i>1){let o=this.detach(i);this.attach(i,s),this.attach(r,o)}else this.attach(i,s)}move(e,t){this.attach(t,this.detach(e))}};function wh(n,e,t,i,r){return n===t&&Object.is(e,i)?1:Object.is(r(n,e),r(t,i))?-1:0}function ST(n,e,t,i){let r,s,o=0,a=n.length-1,c=void 0;if(Array.isArray(e)){De(i);let l=e.length-1;for(De(null);o<=a&&o<=l;){let u=n.at(o),f=e[o],d=wh(o,u,o,f,t);if(d!==0){d<0&&n.updateValue(o,f),o++;continue}let h=n.at(a),g=e[l],x=wh(a,h,l,g,t);if(x!==0){x<0&&n.updateValue(a,g),a--,l--;continue}let m=t(o,u),p=t(a,h),b=t(o,f);if(Object.is(b,p)){let M=t(l,g);Object.is(M,m)?(n.swap(o,a),n.updateValue(a,g),l--,a--):n.move(a,o),n.updateValue(o,f),o++;continue}if(r??=new ll,s??=av(n,o,a,t),Yh(n,r,o,b))n.updateValue(o,f),o++,a++;else if(s.has(b))r.set(m,n.detach(o)),a--;else{let M=n.create(o,e[o]);n.attach(o,M),o++,a++}}for(;o<=l;)ov(n,r,t,o,e[o]),o++}else if(e!=null){De(i);let l=e[Symbol.iterator]();De(null);let u=l.next();for(;!u.done&&o<=a;){let f=n.at(o),d=u.value,h=wh(o,f,o,d,t);if(h!==0)h<0&&n.updateValue(o,d),o++,u=l.next();else{r??=new ll,s??=av(n,o,a,t);let g=t(o,d);if(Yh(n,r,o,g))n.updateValue(o,d),o++,a++,u=l.next();else if(!s.has(g))n.attach(o,n.create(o,d)),o++,a++,u=l.next();else{let x=t(o,f);r.set(x,n.detach(o)),a--}}}for(;!u.done;)ov(n,r,t,n.length,u.value),u=l.next()}for(;o<=a;)n.destroy(n.detach(a--));r?.forEach(l=>{n.destroy(l)})}function Yh(n,e,t,i){return e!==void 0&&e.has(i)?(n.attach(t,e.get(i)),e.delete(i),!0):!1}function ov(n,e,t,i,r){if(Yh(n,e,i,t(i,r)))n.updateValue(i,r);else{let s=n.create(i,r);n.attach(i,s)}}function av(n,e,t,i){let r=new Set;for(let s=e;s<=t;s++)r.add(i(s,n.at(s)));return r}var ll=class{kvMap=new Map;_vMap=void 0;has(e){return this.kvMap.has(e)}delete(e){if(!this.has(e))return!1;let t=this.kvMap.get(e);return this._vMap!==void 0&&this._vMap.has(t)?(this.kvMap.set(e,this._vMap.get(t)),this._vMap.delete(t)):this.kvMap.delete(e),!0}get(e){return this.kvMap.get(e)}set(e,t){if(this.kvMap.has(e)){let i=this.kvMap.get(e);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,t)}else this.kvMap.set(e,t)}forEach(e){for(let[t,i]of this.kvMap)if(e(i,t),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),e(i,t)}}};function ci(n,e,t,i,r,s,o,a){os("NgControlFlow");let c=tt(),l=bn(),u=Mi(l.consts,s);return cl(c,l,n,e,t,i,r,u,256,o,a),Tp}function Tp(n,e,t,i,r,s,o,a){os("NgControlFlow");let c=tt(),l=bn(),u=Mi(l.consts,s);return cl(c,l,n,e,t,i,r,u,512,o,a),Tp}function li(n,e){os("NgControlFlow");let t=tt(),i=Vs(),r=t[i]!==Yn?t[i]:-1,s=r!==-1?ul(t,Kt+r):void 0,o=0;if(Ti(t,i,n)){let a=De(null);try{if(s!==void 0&&by(s,o),n!==-1){let c=Kt+n,l=ul(t,c),u=Qh(t[Le],c),f=Sy(l,u,t),d=El(t,u,e,{dehydratedView:f});bl(l,d,o,$o(u,f))}}finally{De(a)}}else if(s!==void 0){let a=Ey(s,o);a!==void 0&&(a[zt]=e)}}var Zh=class{lContainer;$implicit;$index;constructor(e,t,i){this.lContainer=e,this.$implicit=t,this.$index=i}get $count(){return this.lContainer.length-Pt}};function as(n,e){return e}var Jh=class{hasEmptyBlock;trackByFn;liveCollection;constructor(e,t,i){this.hasEmptyBlock=e,this.trackByFn=t,this.liveCollection=i}};function vt(n,e,t,i,r,s,o,a,c,l,u,f,d){os("NgControlFlow");let h=tt(),g=bn(),x=c!==void 0,m=tt(),p=a?o.bind(m[jn][zt]):o,b=new Jh(x,p);m[Kt+n]=b,cl(h,g,n+1,e,t,i,r,Mi(g.consts,s),256),x&&cl(h,g,n+2,c,l,u,f,Mi(g.consts,d),512)}var Kh=class extends Xh{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(e,t,i){super(),this.lContainer=e,this.hostLView=t,this.templateTNode=i}get length(){return this.lContainer.length-Pt}at(e){return this.getLView(e)[zt].$implicit}attach(e,t){let i=t[Os];this.needsIndexUpdate||=e!==this.length,bl(this.lContainer,t,e,$o(this.templateTNode,i)),TT(this.lContainer,e)}detach(e){return this.needsIndexUpdate||=e!==this.length-1,wT(this.lContainer,e),CT(this.lContainer,e)}create(e,t){let i=Fh(this.lContainer,this.templateTNode.tView.ssrId);return El(this.hostLView,this.templateTNode,new Zh(this.lContainer,t,e),{dehydratedView:i})}destroy(e){_l(e[Le],e)}updateValue(e,t){this.getLView(e)[zt].$implicit=t}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let e=0;e<this.length;e++)this.getLView(e)[zt].$index=e}getLView(e){return IT(this.lContainer,e)}};function yt(n){let e=De(null),t=Wi();try{let i=tt(),r=i[Le],s=i[t],o=t+1,a=ul(i,o);if(s.liveCollection===void 0){let l=Qh(r,o);s.liveCollection=new Kh(a,i,l)}else s.liveCollection.reset();let c=s.liveCollection;if(ST(c,n,s.trackByFn,e),c.updateIndexes(),s.hasEmptyBlock){let l=Vs(),u=c.length===0;if(Ti(i,l,u)){let f=t+2,d=ul(i,f);if(u){let h=Qh(r,f),g=Sy(d,h,i),x=El(i,h,void 0,{dehydratedView:g});bl(d,x,0,$o(h,g))}else r.firstUpdatePass&&MS(d),by(d,0)}}}finally{De(e)}}function ul(n,e){return n[e]}function TT(n,e){if(n.length<=Pt)return;let t=Pt+e,i=n[t],r=i?i[fr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let s=i[Hi];AM(s,r),is.delete(i[zi]),r.detachedLeaveAnimationFns=void 0}}function wT(n,e){if(n.length<=Pt)return;let t=Pt+e,i=n[t],r=i?i[fr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function CT(n,e){return Xo(n,e)}function IT(n,e){return Ey(n,e)}function Qh(n,e){return Nc(n,e)}function ep(n,e,t,i,r){uy(e,n,t,r?"class":"style",i)}function gr(n,e,t,i){let r=tt(),s=r[Le],o=n+Kt,a=s.firstCreatePass?Ay(o,r,2,e,KM,h0(),t,i):s.data[o];if(Jr(a)){let c=r[ii].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(Py(l),()=>(cv(n,e,r,a,i),gr))}}return cv(n,e,r,a,i),gr}function cv(n,e,t,i,r){if(cy(i,t,n,e,Xy),Rc(i)){let s=t[Le];ay(s,t,i),zv(s,i,t)}r!=null&&yp(t,i)}function cs(){let n=bn(),e=qn(),t=ly(e);return n.firstCreatePass&&Dy(n,t),ch(t)&&lh(),ah(),t.classesWithoutHost!=null&&Nb(t)&&ep(n,t,tt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&Pb(t)&&ep(n,t,tt(),t.stylesWithoutHost,!1),cs}function ls(n,e,t,i){return gr(n,e,t,i),cs(),ls}function U(n,e,t,i){let r=tt(),s=r[Le],o=n+Kt,a=s.firstCreatePass?kS(o,s,2,e,t,i):s.data[o];return cy(a,r,n,e,Xy),i!=null&&yp(r,a),U}function z(){let n=qn(),e=ly(n);return ch(e)&&lh(),ah(),z}function an(n,e,t,i){return U(n,e,t,i),z(),an}var Xy=(n,e,t,i,r)=>(zc(!0),$v(e[Nt],i,I0()));function vn(){return tt()}function Rn(n,e,t){let i=tt(),r=Vs();if(Ti(i,r,e)){let s=bn(),o=mh();XM(o,i,n,e,i[Nt],t)}return Rn}var ra="en-US";var AT=ra;function Yy(n){typeof n=="string"&&(AT=n.toLowerCase().replace(/_/g,"-"))}function $i(n,e,t){let i=tt(),r=bn(),s=qn();return DT(r,i,i[Nt],s,n,e,t),$i}function lt(n,e,t){let i=tt(),r=bn(),s=qn();return(s.type&3||t)&&Ry(s,r,i,t,i[Nt],n,e,Zc(s,i,e)),lt}function DT(n,e,t,i,r,s,o){let a=!0,c=null;if((i.type&3||o)&&(c??=Zc(i,e,s),Ry(i,n,e,o,t,r,s,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let d=u[f],h=u[f+1];c??=Zc(i,e,s),nv(i,e,d,h,r,c)}if(l&&l.length)for(let f of l)c??=Zc(i,e,s),nv(i,e,f,r,r,c)}}function Tt(n=1){return C0(n)}function Zn(n,e,t,i){return dT(n,aT(e,t,i)),Zn}function ui(n=1){kc(M0()+n)}function jc(n,e){return n<<17|e<<2}function ss(n){return n>>17&32767}function RT(n){return(n&2)==2}function NT(n,e){return n&131071|e<<17}function tp(n){return n|2}function $s(n){return(n&131068)>>2}function Ch(n,e){return n&-131069|e<<2}function PT(n){return(n&1)===1}function np(n){return n|1}function LT(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=ss(o),c=$s(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let f=t;u=f[1],(u===null||Ps(f,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let d=ss(n[a+1]);n[i+1]=jc(d,a),d!==0&&(n[d+1]=Ch(n[d+1],i)),n[a+1]=NT(n[a+1],i)}else n[i+1]=jc(a,0),a!==0&&(n[a+1]=Ch(n[a+1],i)),a=i;else n[i+1]=jc(c,0),a===0?a=i:n[c+1]=Ch(n[c+1],i),c=i;l&&(n[i+1]=tp(n[i+1])),lv(n,u,i,!0),lv(n,u,i,!1),OT(e,u,n,i,s),o=jc(a,c),s?e.classBindings=o:e.styleBindings=o}function OT(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Ps(s,e)>=0&&(t[i+1]=np(t[i+1]))}function lv(n,e,t,i){let r=n[t+1],s=e===null,o=i?ss(r):$s(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];FT(c,e)&&(a=!0,n[o+1]=i?np(l):tp(l)),o=i?ss(l):$s(l)}a&&(n[t+1]=i?tp(r):np(r))}function FT(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Ps(n,e)>=0:!1}var gn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function kT(n){return n.substring(gn.key,gn.keyEnd)}function UT(n){return n.substring(gn.value,gn.valueEnd)}function BT(n){return VT(n),Zy(n,dl(n,0,gn.textEnd))}function Zy(n,e){let t=gn.textEnd,i=gn.key=dl(n,e,t);return t===i?-1:(i=gn.keyEnd=HT(n,i,t),i=uv(n,i,t,58),i=gn.value=dl(n,i,t),i=gn.valueEnd=zT(n,i,t),uv(n,i,t,59))}function VT(n){gn.key=0,gn.keyEnd=0,gn.value=0,gn.valueEnd=0,gn.textEnd=n.length}function dl(n,e,t){for(;e<t&&n.charCodeAt(e)<=32;)e++;return e}function HT(n,e,t){let i;for(;e<t&&((i=n.charCodeAt(e))===45||i===95||(i&-33)>=65&&(i&-33)<=90||i>=48&&i<=57);)e++;return e}function uv(n,e,t,i){return e=dl(n,e,t),e<t&&e++,e}function zT(n,e,t){let i=-1,r=-1,s=-1,o=e,a=o;for(;o<t;){let c=n.charCodeAt(o++);if(c===59)return a;c===34||c===39?a=o=dv(n,c,o,t):e===o-4&&s===85&&r===82&&i===76&&c===40?a=o=dv(n,41,o,t):c>32&&(a=o),s=r,r=i,i=c&-33}return a}function dv(n,e,t,i){let r=-1,s=t;for(;s<i;){let o=n.charCodeAt(s++);if(o==e&&r!==92)return s;o==92&&r===92?r=0:r=o}throw new Error}function At(n,e,t){return Jy(n,e,t,!1),At}function Ys(n,e){return Jy(n,e,null,!0),Ys}function wp(n){WT(e_,GT,n,!1)}function GT(n,e){for(let t=BT(e);t>=0;t=Zy(e,t))e_(n,kT(e),UT(e))}function Jy(n,e,t,i){let r=tt(),s=bn(),o=Oc(2);if(s.firstUpdatePass&&Qy(s,n,o,i),e!==Yn&&Ti(r,o,e)){let a=s.data[Wi()];t_(s,a,r,r[Nt],n,r[o+1]=JT(e,t),i,o)}}function WT(n,e,t,i){let r=bn(),s=Oc(2);r.firstUpdatePass&&Qy(r,null,s,i);let o=tt();if(t!==Yn&&Ti(o,s,t)){let a=r.data[Wi()];if(n_(a,i)&&!Ky(r,s)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=Ec(c,t||"")),ep(r,a,o,t,i)}else ZT(r,a,o,o[Nt],o[s+1],o[s+1]=YT(n,e,t),i,s)}}function Ky(n,e){return e>=n.expandoStartIndex}function Qy(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Wi()],o=Ky(n,t);n_(s,i)&&e===null&&!o&&(e=!1),e=jT(r,s,e,i),LT(r,s,e,t,o,i)}}function jT(n,e,t,i){let r=b0(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Ih(null,n,e,t,i),t=Ko(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Ih(r,n,e,t,i),s===null){let c=$T(n,e,i);c!==void 0&&Array.isArray(c)&&(c=Ih(null,n,e,c[1],i),c=Ko(c,e.attrs,i),qT(n,e,i,c))}else s=XT(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function $T(n,e,t){let i=t?e.classBindings:e.styleBindings;if($s(i)!==0)return n[ss(i)]}function qT(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[ss(r)]=i}function XT(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=Ko(i,o,t)}return Ko(i,e.attrs,t)}function Ih(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=Ko(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function Ko(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),jf(n,o,t?!0:e[++s]))}return n===void 0?null:n}function YT(n,e,t){if(t==null||t==="")return ti;let i=[],r=ta(t);if(Array.isArray(r))for(let s=0;s<r.length;s++)n(i,r[s],!0);else if(r instanceof Set)for(let s of r)n(i,s,!0);else if(typeof r=="object")for(let s in r)r.hasOwnProperty(s)&&n(i,s,r[s]);else typeof r=="string"&&e(i,r);return i}function e_(n,e,t){jf(n,e,ta(t))}function ZT(n,e,t,i,r,s,o,a){r===Yn&&(r=ti);let c=0,l=0,u=0<r.length?r[0]:null,f=0<s.length?s[0]:null;for(;u!==null||f!==null;){let d=c<r.length?r[c+1]:void 0,h=l<s.length?s[l+1]:void 0,g=null,x;u===f?(c+=2,l+=2,d!==h&&(g=f,x=h)):f===null||u!==null&&u<f?(c+=2,g=u):(l+=2,g=f,x=h),g!==null&&t_(n,e,t,i,g,x,o,a),u=c<r.length?r[c]:null,f=l<s.length?s[l]:null}}function t_(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=PT(l)?fv(c,e,t,r,$s(l),o):void 0;if(!fl(u)){fl(s)||RT(l)&&(s=fv(c,null,t,r,a,o));let f=th(Wi(),t);WM(i,o,f,r,s)}}function fv(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,f=u===null,d=t[r+1];d===Yn&&(d=f?ti:void 0);let h=f?Cc(d,i):u===i?d:void 0;if(l&&!fl(h)&&(h=Cc(c,i)),fl(h)&&(a=h,o))return a;let g=n[r+1];r=o?ss(g):$s(g)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Cc(c,i))}return a}function fl(n){return n!==void 0}function JT(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=xc(ta(n)))),n}function n_(n,e){return(n.flags&(e?8:16))!==0}function j(n,e=""){let t=tt(),i=bn(),r=n+Kt,s=i.firstCreatePass?Ml(i,r,1,e,null):i.data[r],o=KT(i,t,s,e);t[r]=o,Hc()&&gp(i,t,o,s),Bs(s,!1)}var KT=(n,e,t,i)=>(zc(!0),sM(e[Nt],i));function QT(n,e,t,i=""){return Ti(n,Vs(),t)?e+jr(t)+i:Yn}function ew(n,e,t,i,r,s,o,a=""){let c=v0(),l=BS(n,c,t,r,o);return Oc(3),l?e+jr(t)+i+jr(r)+s+jr(o)+a:Yn}function ke(n){return Nn("",n),ke}function Nn(n,e,t){let i=tt(),r=QT(i,n,e,t);return r!==Yn&&tw(i,Wi(),r),Nn}function tw(n,e,t){let i=th(e,n);oM(n[Nt],i,t)}function Cp(n,e,t,i,r,s,o=""){return ew(tt(),n,e,t,i,r,s,o)}var i_=(()=>{class n{applicationErrorHandler=Ie(ts);appRef=Ie(Cl);taskService=Ie(Hs);ngZone=Ie(mn);zonelessEnabled=Ie(Ho);tracing=Ie(vr,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pn;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(No):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(Ie(Eh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?P0:gh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(No+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ht({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function r_(){return[{provide:ur,useExisting:i_},{provide:mn,useClass:Po},{provide:Ho,useValue:!0}]}function nw(){return typeof $localize<"u"&&$localize.locale||ra}var Ip=new st("",{factory:()=>Ie(Ip,{optional:!0,skipSelf:!0})||nw()});var Il=class{destroyed=!1;listeners=null;errorHandler=Ie(zn,{optional:!0});destroyRef=Ie(It);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(e){if(this.destroyed)throw new Xe(953,!1);return(this.listeners??=[]).push(e),{unsubscribe:()=>{let t=this.listeners?.indexOf(e);t!==void 0&&t!==-1&&this.listeners?.splice(t,1)}}}emit(e){if(this.destroyed){console.warn(Lo(953,!1));return}if(this.listeners===null)return;let t=De(null);try{for(let i of this.listeners)try{i(e)}catch(r){this.errorHandler?.handleError(r)}}finally{De(t)}}};var a_=Symbol("InputSignalNode#UNSET"),cw=Vt(St({},sc),{transformFn:void 0,applyValueToInputSignal(n,e){wo(n,e)}});function c_(n,e){let t=Object.create(cw);t.value=n,t.transformFn=e?.transform;function i(){if(Mo(t),t.value===a_){let r=null;throw new Xe(-950,r)}return t.value}return i[rn]=t,i}function l_(n){return new Il}function s_(n,e){return c_(n,e)}function lw(n){return c_(a_,n)}var Dl=(s_.required=lw,s_);function o_(n,e){return Vy(e)}function uw(n,e){return Hy(e)}var yn=(o_.required=uw,o_);var Ap=new st(""),dw=new st("");function sa(n){return!n.moduleRef}function fw(n){let e=sa(n)?n.r3Injector:n.moduleRef.injector,t=e.get(mn);return t.run(()=>{sa(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ts),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),sa(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Ap);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Ap);o.add(s),n.moduleRef.onDestroy(()=>{Wo(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return pw(i,t,()=>{let s=e.get(Hs),o=s.add(),a=e.get(Sp);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(Ip,ra);if(Yy(c||ra),!e.get(dw,!0))return sa(n)?e.get(Cl):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(sa(n)){let u=e.get(Cl);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return hw?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var hw;function pw(n,e,t){try{let i=t();return Mp(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Al=null;function mw(n=[],e){return Ei.create({name:e,providers:[{provide:Fo,useValue:"platform"},{provide:Ap,useValue:new Set([()=>Al=null])},...n]})}function gw(n=[]){if(Al)return Al;let e=mw(n);return Al=e,$y(),vw(e),e}function vw(n){let e=n.get(ml,null);Ac(n,()=>{e?.forEach(t=>t())})}var yw=1e4;var n4=yw-1e3;function u_(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;ot(nt.BootstrapApplicationStart);try{let s=r?.injector??gw(i),o=[r_(),O0,...t||[]],a=new Jo({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return fw({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{ot(nt.BootstrapApplicationEnd)}}var d_=null;function aa(){return d_}function Dp(n){d_??=n}var oa=class{};function Rp(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var ca=class{};var f_="browser";var la=class{_doc;constructor(e){this._doc=e}manager},Rl=(()=>{class n extends la{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(gt(Xn))};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})(),Ll=new st(""),Op=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof Rl));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof Rl);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Xe(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(gt(Ll),gt(mn))};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})(),Np="ng-app-id";function h_(n){for(let e of n)e.remove()}function p_(n,e){let t=e.createElement("style");return t.textContent=n,t}function _w(n,e,t,i){let r=n.head?.querySelectorAll(`style[${Np}="${e}"],link[${Np}="${e}"]`);if(r)for(let s of r)s.removeAttribute(Np),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function Lp(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Fp=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,_w(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,p_);i?.forEach(r=>this.addUsage(r,this.external,Lp))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(h_(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])h_(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,p_(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,Lp(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(gt(Xn),gt(pl),gt(gl,8),gt(ea))};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})(),Pp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},kp=/%COMP%/g;var g_="%COMP%",xw=`_nghost-${g_}`,Ew=`_ngcontent-${g_}`,bw=!0,Mw=new st("",{factory:()=>bw});function Sw(n){return Ew.replace(kp,n)}function Tw(n){return xw.replace(kp,n)}function v_(n,e){return e.map(t=>t.replace(kp,n))}var Up=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new ua(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Pl?r.applyToHost(t):r instanceof da&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case ai.Emulated:s=new Pl(c,l,i,this.appId,u,o,a,f);break;case ai.ShadowDom:return new Nl(c,t,i,o,a,this.nonce,f,l);case ai.ExperimentalIsolatedShadowDom:return new Nl(c,t,i,o,a,this.nonce,f);default:s=new da(c,l,i,u,o,a,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(gt(Op),gt(Fp),gt(pl),gt(Mw),gt(Xn),gt(mn),gt(gl),gt(vr,8))};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})(),ua=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(Pp[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(m_(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(m_(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Xe(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Pp[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Pp[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(ji.DashCase|ji.Important)?e.style.setProperty(t,i,r&ji.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&ji.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=aa().getGlobalEventTarget(this.doc,e),!e))throw new Xe(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function m_(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Nl=class extends ua{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=v_(i.id,l);for(let f of l){let d=document.createElement("style");o&&d.setAttribute("nonce",o),d.textContent=f,this.shadowRoot.appendChild(d)}let u=i.getExternalStyles?.();if(u)for(let f of u){let d=Lp(f,r);o&&d.setAttribute("nonce",o),this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},da=class extends ua{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?v_(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&is.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Pl=class extends da{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=Sw(l),this.hostAttr=Tw(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Ol=class n extends oa{supportsDOMEvents=!0;static makeCurrent(){Dp(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=ww();return t==null?null:Cw(t)}resetBaseElement(){fa=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Rp(document.cookie,e)}},fa=null;function ww(){return fa=fa||document.head.querySelector("base"),fa?fa.getAttribute("href"):null}function Cw(n){return new URL(n,document.baseURI).pathname}var Iw=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})(),y_=["alt","control","meta","shift"],Aw={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},Dw={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},__=(()=>{class n extends la{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>aa().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),y_.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=Aw[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),y_.forEach(o=>{if(o!==r){let a=Dw[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(gt(Xn))};static \u0275prov=Ht({token:n,factory:n.\u0275fac})}return n})();async function Bp(n,e,t){let i=St({rootComponent:n},Rw(e,t));return u_(i)}function Rw(n,e){return{platformRef:e?.platformRef,appProviders:[...Fw,...n?.providers??[]],platformProviders:Ow}}function Nw(){Ol.makeCurrent()}function Pw(){return new zn}function Lw(){return sp(document),document}var Ow=[{provide:ea,useValue:f_},{provide:ml,useValue:Nw,multi:!0},{provide:Xn,useFactory:Lw}];var Fw=[{provide:Fo,useValue:"root"},{provide:zn,useFactory:Pw},{provide:Ll,useClass:Rl,multi:!0},{provide:Ll,useClass:__,multi:!0},Up,Fp,Op,{provide:rs,useExisting:Up},{provide:ca,useClass:Iw},[]];var x_={providers:[_h()]};var kw=["overlay"],Uw=(n,e)=>e.id;function Bw(n,e){if(n&1&&an(0,"div",11),n&2){let t=e.$implicit;At("width",t.size,"px")("height",t.size,"px")("left",t.left,"%")("top",t.top,"%")("--dur",t.dur)("--delay",t.delay)}}var Fl=class n{onDone=l_();overlay=yn("overlay");destroyRef=Ie(It);particles=Array.from({length:18},(e,t)=>({id:t,size:Math.random()*6+3,left:Math.random()*100,top:Math.random()*100,dur:`${Math.random()*6+5}s`,delay:`${Math.random()*4}s`}));constructor(){let e=setTimeout(()=>{let t=this.overlay()?.nativeElement;t&&(t.classList.add("hidden"),setTimeout(()=>this.onDone.emit(),900))},2400);this.destroyRef.onDestroy(()=>clearTimeout(e))}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-loading-screen"]],viewQuery:function(t,i){t&1&&Zn(i.overlay,kw,5),t&2&&ui()},outputs:{onDone:"onDone"},decls:16,vars:0,consts:[["overlay",""],[1,"loader-overlay"],[1,"loader-particles"],[1,"loader-particle",3,"width","height","left","top","--dur","--delay"],[2,"text-align","center","position","relative","z-index","2"],[2,"font-size","3.5rem","margin-bottom","1rem","animation","iconFloat 2s ease-in-out infinite","display","inline-block"],[1,"loader-logo"],[1,"loader-subtitle"],[1,"loader-bar-container"],[1,"loader-bar"],[1,"loader-text"],[1,"loader-particle"]],template:function(t,i){t&1&&(U(0,"div",1,0)(2,"div",2),vt(3,Bw,1,12,"div",3,Uw),z(),U(5,"div",4)(6,"div",5),j(7,"\u{1F48A}"),z(),U(8,"div",6),j(9,"\u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628"),z(),U(10,"div",7),j(11,"\u0644\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629"),z(),U(12,"div",8),an(13,"div",9),z(),U(14,"div",10),j(15,"\u062C\u0627\u0631\u064A \u0627\u0644\u062A\u062D\u0645\u064A\u0644..."),z()()()),t&2&&(oe(3),yt(i.particles))},styles:["@keyframes _ngcontent-%COMP%_iconFloat{0%,to{transform:translateY(0) rotate(0)}50%{transform:translateY(-10px) rotate(5deg)}}"]})};var Vw=["dot"],Hw=["ring"],kl=class n{dot=yn("dot");ring=yn("ring");mouseX=0;mouseY=0;ringX=0;ringY=0;rafId=0;destroyRef=Ie(It);onMouseMove(e){this.mouseX=e.clientX,this.mouseY=e.clientY,this.dot()?.nativeElement.style.setProperty("left",`${e.clientX}px`),this.dot()?.nativeElement.style.setProperty("top",`${e.clientY}px`)}onMouseOver(e){let i=!!e.target.closest('a, button, .glass-card, .filter-btn, .partner-logo-item, .social-btn, .nav-link, .footer-link, [role="button"]');this.dot()?.nativeElement.classList.toggle("hovering",i),this.ring()?.nativeElement.classList.toggle("hovering",i)}constructor(){if(window.matchMedia("(max-width: 768px)").matches)return;let t=()=>{this.ringX+=(this.mouseX-this.ringX)*.13,this.ringY+=(this.mouseY-this.ringY)*.13;let i=this.ring()?.nativeElement;i&&(i.style.left=`${this.ringX}px`,i.style.top=`${this.ringY}px`),this.rafId=requestAnimationFrame(t)};this.rafId=requestAnimationFrame(t),this.destroyRef.onDestroy(()=>{cancelAnimationFrame(this.rafId)})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-custom-cursor"]],viewQuery:function(t,i){t&1&&Zn(i.dot,Vw,5)(i.ring,Hw,5),t&2&&ui(2)},hostBindings:function(t,i){t&1&&$i("mousemove",function(s){return i.onMouseMove(s)},na)("mouseover",function(s){return i.onMouseOver(s)},cp)},decls:4,vars:0,consts:[["dot",""],["ring",""],[1,"cursor-dot"],[1,"cursor-ring"]],template:function(t,i){t&1&&an(0,"div",2,0)(2,"div",3,1)},encapsulation:2})};var E_=(n,e)=>e.href;function zw(n,e){if(n&1){let t=vn();U(0,"a",11),lt("click",function(r){let s=sn(t).$implicit,o=Tt();return on(o.scrollTo(r,s.href))}),j(1),z()}if(n&2){let t=e.$implicit;Rn("href",t.href,Xs),oe(),ke(t.label)}}function Gw(n,e){if(n&1){let t=vn();U(0,"a",14),lt("click",function(r){let s=sn(t).$implicit,o=Tt(2);return on(o.scrollTo(r,s.href))}),j(1),z()}if(n&2){let t=e.$implicit;Rn("href",t.href,Xs),oe(),ke(t.label)}}function Ww(n,e){if(n&1){let t=vn();U(0,"div",10),vt(1,Gw,2,2,"a",12,E_),U(3,"a",13),lt("click",function(r){sn(t);let s=Tt();return on(s.scrollTo(r,"#contact"))}),j(4,"\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0646\u0627"),z()()}if(n&2){let t=Tt();oe(),yt(t.navLinks)}}var jw=[{href:"#hero",label:"\u0627\u0644\u0631\u0626\u064A\u0633\u064A\u0629"},{href:"#about",label:"\u0645\u0646 \u0646\u062D\u0646"},{href:"#products",label:"\u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"},{href:"#services",label:"\u062E\u062F\u0645\u0627\u062A\u0646\u0627"},{href:"#partners",label:"\u0627\u0644\u0634\u0631\u0643\u0627\u0621"},{href:"#contact",label:"\u0627\u062A\u0635\u0644 \u0628\u0646\u0627"}],Ul=class n{navLinks=jw;scrolled=Yt(!1);mobileOpen=Yt(!1);onScroll(){this.scrolled.set(window.scrollY>60)}toggleMobile(){this.mobileOpen.update(e=>!e)}scrollTo(e,t){e.preventDefault(),document.querySelector(t)?.scrollIntoView({behavior:"smooth"}),this.mobileOpen.set(!1)}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-navbar"]],hostBindings:function(t,i){t&1&&$i("scroll",function(){return i.onScroll()},na)},decls:18,vars:6,consts:[[1,"navbar"],[2,"max-width","1300px","margin","0 auto","display","flex","align-items","center","justify-content","space-between","gap","2rem"],["href","#hero",2,"display","flex","align-items","center","gap","0.6rem","text-decoration","none","flex-shrink","0",3,"click"],[2,"width","40px","height","40px","border-radius","12px","background","linear-gradient(135deg, #2D9CDB, #1a7ab5)","display","flex","align-items","center","justify-content","center","font-size","1.3rem","box-shadow","0 4px 15px rgba(45,156,219,0.45)"],[2,"color","#B8E4F9","font-weight","900","font-size","1.05rem","line-height","1.2","letter-spacing","0.02em"],[2,"color","rgba(184,228,249,0.55)","font-size","0.65rem","font-weight","400"],[1,"desktop-nav",2,"display","flex","gap","2rem","align-items","center"],[1,"nav-link",3,"href"],["href","#contact",1,"btn-primary","desktop-nav",2,"font-size","0.9rem","padding","0.65rem 1.6rem",3,"click"],[1,"mobile-menu-btn",2,"background","none","border","1.5px solid rgba(184,228,249,0.35)","border-radius","10px","padding","0.45rem 0.7rem","cursor","none","display","none","color","#B8E4F9","font-size","1.2rem",3,"click"],[2,"position","absolute","top","100%","right","0","left","0","background","rgba(10,22,40,0.97)","backdrop-filter","blur(20px)","border-top","1px solid rgba(184,228,249,0.12)","padding","1.5rem","display","flex","flex-direction","column","gap","0.8rem"],[1,"nav-link",3,"click","href"],[1,"nav-link",2,"padding","0.5rem 0","font-size","1rem",3,"href"],["href","#contact",1,"btn-primary",2,"text-align","center","margin-top","0.5rem",3,"click"],[1,"nav-link",2,"padding","0.5rem 0","font-size","1rem",3,"click","href"]],template:function(t,i){t&1&&(U(0,"nav",0)(1,"div",1)(2,"a",2),lt("click",function(s){return i.scrollTo(s,"#hero")}),U(3,"div",3),j(4,"\u{1F48A}"),z(),U(5,"div")(6,"div",4),j(7,"\u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628"),z(),U(8,"div",5),j(9,"\u0644\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629"),z()()(),U(10,"div",6),vt(11,zw,2,2,"a",7,E_),z(),U(13,"a",8),lt("click",function(s){return i.scrollTo(s,"#contact")}),j(14,"\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0646\u0627"),z(),U(15,"button",9),lt("click",function(){return i.toggleMobile()}),j(16),z()(),ci(17,Ww,5,0,"div",10),z()),t&2&&(At("background",i.scrolled()?void 0:"transparent"),Ys("scrolled",i.scrolled()),oe(11),yt(i.navLinks),oe(5),ke(i.mobileOpen()?"\u2715":"\u2630"),oe(),li(i.mobileOpen()?17:-1))},styles:["@media(max-width:900px){.desktop-nav[_ngcontent-%COMP%]{display:none!important}.mobile-menu-btn[_ngcontent-%COMP%]{display:block!important}}"]})};var $_=0,xm=1,q_=2;var Va=1,X_=2,vo=3,Qi=0,xn=1,Ni=2,Pi=0,gs=1,Em=2,bm=3,Mm=4,Y_=5;var Tr=100,Z_=101,J_=102,K_=103,Q_=104,ex=200,tx=201,nx=202,ix=203,uu=204,du=205,rx=206,sx=207,ox=208,ax=209,cx=210,lx=211,ux=212,dx=213,fx=214,fu=0,hu=1,pu=2,vs=3,mu=4,gu=5,vu=6,yu=7,Sm=0,hx=1,px=2,mi=0,Tm=1,wm=2,Cm=3,Im=4,Am=5,Dm=6,Rm=7;var um=300,Rr=301,ys=302,ju=303,$u=304,Ha=306,_u=1e3,Ii=1001,xu=1002,Jt=1003,mx=1004;var za=1005;var en=1006,qu=1007;var Nr=1008;var Bn=1009,Nm=1010,Pm=1011,yo=1012,Xu=1013,gi=1014,vi=1015,Li=1016,Yu=1017,Zu=1018,_o=1020,Lm=35902,Om=35899,Fm=1021,km=1022,Qn=1023,Ai=1026,Pr=1027,Um=1028,Ju=1029,Lr=1030,Ku=1031;var Qu=1033,Ga=33776,Wa=33777,ja=33778,$a=33779,ed=35840,td=35841,nd=35842,id=35843,rd=36196,sd=37492,od=37496,ad=37488,cd=37489,qa=37490,ld=37491,ud=37808,dd=37809,fd=37810,hd=37811,pd=37812,md=37813,gd=37814,vd=37815,yd=37816,_d=37817,xd=37818,Ed=37819,bd=37820,Md=37821,Sd=36492,Td=36494,wd=36495,Cd=36283,Id=36284,Xa=36285,Ad=36286;var _a=2300,Eu=2301,lu=2302,dm=2303,fm=2400,hm=2401,pm=2402;var gx=3200;var Bm=0,vx=1,nr="",On="srgb",xa="srgb-linear",Ea="linear",at="srgb";var ps=7680;var mm=519,yx=512,_x=513,xx=514,Dd=515,Ex=516,bx=517,Rd=518,Mx=519,gm=35044;var Vm="300 es",pi=2e3,ba=2001;function $w(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function qw(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Ma(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function Sx(){let n=Ma("canvas");return n.style.display="block",n}var b_={},lo=null;function Hm(...n){let e="THREE."+n.shift();lo?lo("log",e,...n):console.log(e,...n)}function Tx(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Ae(...n){n=Tx(n);let e="THREE."+n.shift();if(lo)lo("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Ne(...n){n=Tx(n);let e="THREE."+n.shift();if(lo)lo("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function bu(...n){let e=n.join(" ");e in b_||(b_[e]=!0,Ae(...n))}function wx(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var Cx={[fu]:hu,[pu]:vu,[mu]:yu,[vs]:gu,[hu]:fu,[vu]:pu,[yu]:mu,[gu]:vs},Di=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},ln=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var Vp=Math.PI/180,Mu=180/Math.PI;function Ya(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(ln[n&255]+ln[n>>8&255]+ln[n>>16&255]+ln[n>>24&255]+"-"+ln[e&255]+ln[e>>8&255]+"-"+ln[e>>16&15|64]+ln[e>>24&255]+"-"+ln[t&63|128]+ln[t>>8&255]+"-"+ln[t>>16&255]+ln[t>>24&255]+ln[i&255]+ln[i>>8&255]+ln[i>>16&255]+ln[i>>24&255]).toLowerCase()}function Ke(n,e,t){return Math.max(e,Math.min(t,n))}function Xw(n,e){return(n%e+e)%e}function Hp(n,e,t){return(1-t)*n+t*e}function ha(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Sn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var ut=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Ri=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],f=i[r+3],d=s[o+0],h=s[o+1],g=s[o+2],x=s[o+3];if(f!==x||c!==d||l!==h||u!==g){let m=c*d+l*h+u*g+f*x;m<0&&(d=-d,h=-h,g=-g,x=-x,m=-m);let p=1-a;if(m<.9995){let b=Math.acos(m),M=Math.sin(b);p=Math.sin(p*b)/M,a=Math.sin(a*b)/M,c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a}else{c=c*p+d*a,l=l*p+h*a,u=u*p+g*a,f=f*p+x*a;let b=1/Math.sqrt(c*c+l*l+u*u+f*f);c*=b,l*=b,u*=b,f*=b}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=f}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],f=s[o],d=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*f+c*h-l*d,e[t+1]=c*g+u*d+l*f-a*h,e[t+2]=l*g+u*h+a*d-c*f,e[t+3]=u*g-a*f-c*d-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),f=a(s/2),d=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"YXZ":this._x=d*u*f+l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"ZXY":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f-d*h*g;break;case"ZYX":this._x=d*u*f-l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f+d*h*g;break;case"YZX":this._x=d*u*f+l*h*g,this._y=l*h*f+d*u*g,this._z=l*u*g-d*h*f,this._w=l*u*f-d*h*g;break;case"XZY":this._x=d*u*f-l*h*g,this._y=l*h*f-d*u*g,this._z=l*u*g+d*h*f,this._w=l*u*f+d*h*g;break;default:Ae("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],f=t[10],d=i+a+f;if(d>0){let h=.5/Math.sqrt(d+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>f){let h=2*Math.sqrt(1+i-a-f);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>f){let h=2*Math.sqrt(1+a-i-f);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+f-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Ke(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},B=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(M_.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(M_.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),f=2*(s*i-o*t);return this.x=t+c*l+o*f-a*u,this.y=i+c*u+a*l-s*f,this.z=r+c*f+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return zp.copy(this).projectOnVector(e),this.sub(zp)}reflect(e){return this.sub(zp.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Ke(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},zp=new B,M_=new Ri,Ue=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],f=i[7],d=i[2],h=i[5],g=i[8],x=r[0],m=r[3],p=r[6],b=r[1],M=r[4],S=r[7],D=r[2],T=r[5],I=r[8];return s[0]=o*x+a*b+c*D,s[3]=o*m+a*M+c*T,s[6]=o*p+a*S+c*I,s[1]=l*x+u*b+f*D,s[4]=l*m+u*M+f*T,s[7]=l*p+u*S+f*I,s[2]=d*x+h*b+g*D,s[5]=d*m+h*M+g*T,s[8]=d*p+h*S+g*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=u*o-a*l,d=a*c-u*s,h=l*s-o*c,g=t*f+i*d+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let x=1/g;return e[0]=f*x,e[1]=(r*l-u*i)*x,e[2]=(a*i-r*o)*x,e[3]=d*x,e[4]=(u*t-r*c)*x,e[5]=(r*s-a*t)*x,e[6]=h*x,e[7]=(i*c-l*t)*x,e[8]=(o*t-i*s)*x,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Gp.makeScale(e,t)),this}rotate(e){return this.premultiply(Gp.makeRotation(-e)),this}translate(e,t){return this.premultiply(Gp.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Gp=new Ue,S_=new Ue().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),T_=new Ue().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function Yw(){let n={enabled:!0,workingColorSpace:xa,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===at&&(r.r=Ki(r.r),r.g=Ki(r.g),r.b=Ki(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===at&&(r.r=co(r.r),r.g=co(r.g),r.b=co(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===nr?Ea:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return bu("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return bu("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[xa]:{primaries:e,whitePoint:i,transfer:Ea,toXYZ:S_,fromXYZ:T_,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:On},outputColorSpaceConfig:{drawingBufferColorSpace:On}},[On]:{primaries:e,whitePoint:i,transfer:at,toXYZ:S_,fromXYZ:T_,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:On}}}),n}var Je=Yw();function Ki(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function co(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var Zs,Su=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{Zs===void 0&&(Zs=Ma("canvas")),Zs.width=e.width,Zs.height=e.height;let r=Zs.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=Zs}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Ma("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=Ki(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Ki(t[i]/255)*255):t[i]=Ki(t[i]);return{data:t,width:e.width,height:e.height}}else return Ae("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},Zw=0,uo=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:Zw++}),this.uuid=Ya(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Wp(r[o].image)):s.push(Wp(r[o]))}else s=Wp(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Wp(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Su.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Ae("Texture: Unable to serialize Texture."),{})}var Jw=0,jp=new B,ir=(()=>{class n extends Di{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Ii,s=Ii,o=en,a=Nr,c=Qn,l=Bn,u=n.DEFAULT_ANISOTROPY,f=nr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:Jw++}),this.uuid=Ya(),this.name="",this.source=new uo(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new ut(0,0),this.repeat=new ut(1,1),this.center=new ut(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Ue,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=f,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(jp).x}get height(){return this.source.getSize(jp).y}get depth(){return this.source.getSize(jp).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Ae(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){Ae(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==um)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case _u:t.x=t.x-Math.floor(t.x);break;case Ii:t.x=t.x<0?0:1;break;case xu:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case _u:t.y=t.y-Math.floor(t.y);break;case Ii:t.y=t.y<0?0:1;break;case xu:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=um,n.DEFAULT_ANISOTROPY=1,n})(),Ft=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],f=c[8],d=c[1],h=c[5],g=c[9],x=c[2],m=c[6],p=c[10];if(Math.abs(u-d)<.01&&Math.abs(f-x)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+d)<.1&&Math.abs(f+x)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let M=(l+1)/2,S=(h+1)/2,D=(p+1)/2,T=(u+d)/4,I=(f+x)/4,y=(g+m)/4;return M>S&&M>D?M<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(M),r=T/i,s=I/i):S>D?S<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(S),i=T/r,s=y/r):D<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(D),i=I/s,r=y/s),this.set(i,r,s,t),this}let b=Math.sqrt((m-g)*(m-g)+(f-x)*(f-x)+(d-u)*(d-u));return Math.abs(b)<.001&&(b=1),this.x=(m-g)/b,this.y=(f-x)/b,this.z=(d-u)/b,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Ke(this.x,e.x,t.x),this.y=Ke(this.y,e.y,t.y),this.z=Ke(this.z,e.z,t.z),this.w=Ke(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=Ke(this.x,e,t),this.y=Ke(this.y,e,t),this.z=Ke(this.z,e,t),this.w=Ke(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Ke(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Tu=class extends Di{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:en,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Ft(0,0,e,t),this.scissorTest=!1,this.viewport=new Ft(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new ir(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:en,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new uo(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},Fn=class extends Tu{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Sa=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Jt,this.minFilter=Jt,this.wrapR=Ii,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var wu=class extends ir{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Jt,this.minFilter=Jt,this.wrapR=Ii,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Ot=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m)}set(e,t,i,r,s,o,a,c,l,u,f,d,h,g,x,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=f,p[14]=d,p[3]=h,p[7]=g,p[11]=x,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/Js.setFromMatrixColumn(e,0).length(),s=1/Js.setFromMatrixColumn(e,1).length(),o=1/Js.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),f=Math.sin(s);if(e.order==="XYZ"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=-c*f,t[8]=l,t[1]=h+g*l,t[5]=d-x*l,t[9]=-a*c,t[2]=x-d*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d+x*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*f,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=x+d*a,t[10]=o*c}else if(e.order==="ZXY"){let d=c*u,h=c*f,g=l*u,x=l*f;t[0]=d-x*a,t[4]=-o*f,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=x-d*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let d=o*u,h=o*f,g=a*u,x=a*f;t[0]=c*u,t[4]=g*l-h,t[8]=d*l+x,t[1]=c*f,t[5]=x*l+d,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=x-d*f,t[8]=g*f+h,t[1]=f,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*f+g,t[10]=d-x*f}else if(e.order==="XZY"){let d=o*c,h=o*l,g=a*c,x=a*l;t[0]=c*u,t[4]=-f,t[8]=l*u,t[1]=d*f+x,t[5]=o*u,t[9]=h*f-g,t[2]=g*f-h,t[6]=a*u,t[10]=x*f+d}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(Kw,e,Qw)}lookAt(e,t,i){let r=this.elements;return Pn.subVectors(e,t),Pn.lengthSq()===0&&(Pn.z=1),Pn.normalize(),yr.crossVectors(i,Pn),yr.lengthSq()===0&&(Math.abs(i.z)===1?Pn.x+=1e-4:Pn.z+=1e-4,Pn.normalize(),yr.crossVectors(i,Pn)),yr.normalize(),Bl.crossVectors(Pn,yr),r[0]=yr.x,r[4]=Bl.x,r[8]=Pn.x,r[1]=yr.y,r[5]=Bl.y,r[9]=Pn.y,r[2]=yr.z,r[6]=Bl.z,r[10]=Pn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],f=i[5],d=i[9],h=i[13],g=i[2],x=i[6],m=i[10],p=i[14],b=i[3],M=i[7],S=i[11],D=i[15],T=r[0],I=r[4],y=r[8],w=r[12],O=r[1],C=r[5],L=r[9],$=r[13],X=r[2],P=r[6],W=r[10],k=r[14],te=r[3],ne=r[7],he=r[11],be=r[15];return s[0]=o*T+a*O+c*X+l*te,s[4]=o*I+a*C+c*P+l*ne,s[8]=o*y+a*L+c*W+l*he,s[12]=o*w+a*$+c*k+l*be,s[1]=u*T+f*O+d*X+h*te,s[5]=u*I+f*C+d*P+h*ne,s[9]=u*y+f*L+d*W+h*he,s[13]=u*w+f*$+d*k+h*be,s[2]=g*T+x*O+m*X+p*te,s[6]=g*I+x*C+m*P+p*ne,s[10]=g*y+x*L+m*W+p*he,s[14]=g*w+x*$+m*k+p*be,s[3]=b*T+M*O+S*X+D*te,s[7]=b*I+M*C+S*P+D*ne,s[11]=b*y+M*L+S*W+D*he,s[15]=b*w+M*$+S*k+D*be,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],f=e[6],d=e[10],h=e[14],g=e[3],x=e[7],m=e[11],p=e[15],b=c*h-l*d,M=a*h-l*f,S=a*d-c*f,D=o*h-l*u,T=o*d-c*u,I=o*f-a*u;return t*(x*b-m*M+p*S)-i*(g*b-m*D+p*T)+r*(g*M-x*D+p*I)-s*(g*S-x*T+m*I)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=e[9],d=e[10],h=e[11],g=e[12],x=e[13],m=e[14],p=e[15],b=t*a-i*o,M=t*c-r*o,S=t*l-s*o,D=i*c-r*a,T=i*l-s*a,I=r*l-s*c,y=u*x-f*g,w=u*m-d*g,O=u*p-h*g,C=f*m-d*x,L=f*p-h*x,$=d*p-h*m,X=b*$-M*L+S*C+D*O-T*w+I*y;if(X===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/X;return e[0]=(a*$-c*L+l*C)*P,e[1]=(r*L-i*$-s*C)*P,e[2]=(x*I-m*T+p*D)*P,e[3]=(d*T-f*I-h*D)*P,e[4]=(c*O-o*$-l*w)*P,e[5]=(t*$-r*O+s*w)*P,e[6]=(m*S-g*I-p*M)*P,e[7]=(u*I-d*S+h*M)*P,e[8]=(o*L-a*O+l*y)*P,e[9]=(i*O-t*L-s*y)*P,e[10]=(g*T-x*S+p*b)*P,e[11]=(f*S-u*T-h*b)*P,e[12]=(a*w-o*C-c*y)*P,e[13]=(t*C-i*w+r*y)*P,e[14]=(x*M-g*D-m*b)*P,e[15]=(u*D-f*M+d*b)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,f=a+a,d=s*l,h=s*u,g=s*f,x=o*u,m=o*f,p=a*f,b=c*l,M=c*u,S=c*f,D=i.x,T=i.y,I=i.z;return r[0]=(1-(x+p))*D,r[1]=(h+S)*D,r[2]=(g-M)*D,r[3]=0,r[4]=(h-S)*T,r[5]=(1-(d+p))*T,r[6]=(m+b)*T,r[7]=0,r[8]=(g+M)*I,r[9]=(m-b)*I,r[10]=(1-(d+x))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=Js.set(r[0],r[1],r[2]).length(),a=Js.set(r[4],r[5],r[6]).length(),c=Js.set(r[8],r[9],r[10]).length();s<0&&(o=-o),di.copy(this);let l=1/o,u=1/a,f=1/c;return di.elements[0]*=l,di.elements[1]*=l,di.elements[2]*=l,di.elements[4]*=u,di.elements[5]*=u,di.elements[6]*=u,di.elements[8]*=f,di.elements[9]*=f,di.elements[10]*=f,t.setFromRotationMatrix(di),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=pi,c=!1){let l=this.elements,u=2*s/(t-e),f=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),g,x;if(c)g=s/(o-s),x=o*s/(o-s);else if(a===pi)g=-(o+s)/(o-s),x=-2*o*s/(o-s);else if(a===ba)g=-o/(o-s),x=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=f,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=pi,c=!1){let l=this.elements,u=2/(t-e),f=2/(i-r),d=-(t+e)/(t-e),h=-(i+r)/(i-r),g,x;if(c)g=1/(o-s),x=o/(o-s);else if(a===pi)g=-2/(o-s),x=-(o+s)/(o-s);else if(a===ba)g=-1/(o-s),x=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=d,l[1]=0,l[5]=f,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=x,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Js=new B,di=new Ot,Kw=new B(0,0,0),Qw=new B(1,1,1),yr=new B,Bl=new B,Pn=new B,w_=new Ot,C_=new Ri,Ta=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],f=s[9],d=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(Ke(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-f,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Ke(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-d,o),this._z=0);break;case"ZXY":this._x=Math.asin(Ke(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-d,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-Ke(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Ke(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-f,u),this._y=Math.atan2(-d,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-Ke(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-f,g),this._y=0);break;default:Ae("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return w_.makeRotationFromQuaternion(t),this.setFromRotationMatrix(w_,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return C_.setFromEuler(this),this.setFromQuaternion(C_,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),wa=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},eC=0,I_=new B,Ks=new Ri,qi=new Ot,Vl=new B,pa=new B,tC=new B,nC=new Ri,A_=new B(1,0,0),D_=new B(0,1,0),R_=new B(0,0,1),N_={type:"added"},iC={type:"removed"},Qs={type:"childadded",child:null},$p={type:"childremoved",child:null},rr=(()=>{class n extends Di{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:eC++}),this.uuid=Ya(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new B,i=new Ta,r=new Ri,s=new B(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Ot},normalMatrix:{value:new Ue}}),this.matrix=new Ot,this.matrixWorld=new Ot,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new wa,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return Ks.setFromAxisAngle(t,i),this.quaternion.multiply(Ks),this}rotateOnWorldAxis(t,i){return Ks.setFromAxisAngle(t,i),this.quaternion.premultiply(Ks),this}rotateX(t){return this.rotateOnAxis(A_,t)}rotateY(t){return this.rotateOnAxis(D_,t)}rotateZ(t){return this.rotateOnAxis(R_,t)}translateOnAxis(t,i){return I_.copy(t).applyQuaternion(this.quaternion),this.position.add(I_.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(A_,t)}translateY(t){return this.translateOnAxis(D_,t)}translateZ(t){return this.translateOnAxis(R_,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(qi.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Vl.copy(t):Vl.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),pa.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?qi.lookAt(pa,Vl,this.up):qi.lookAt(Vl,pa,this.up),this.quaternion.setFromRotationMatrix(qi),s&&(qi.extractRotation(s.matrixWorld),Ks.setFromRotationMatrix(qi),this.quaternion.premultiply(Ks.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Ne("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(N_),Qs.child=t,this.dispatchEvent(Qs),Qs.child=null):Ne("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(iC),$p.child=t,this.dispatchEvent($p),$p.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),qi.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),qi.multiply(t.parent.matrixWorld)),t.applyMatrix4(qi),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(N_),Qs.child=t,this.dispatchEvent(Qs),Qs.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(pa,t,tC),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(pa,nC,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>Vt(St({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>St({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,f=l.length;u<f;u++){let d=l[u];o(t.shapes,d)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),f=a(t.images),d=a(t.shapes),h=a(t.skeletons),g=a(t.animations),x=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),f.length>0&&(r.images=f),d.length>0&&(r.shapes=d),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),x.length>0&&(r.nodes=x)}return r.object=s,r;function a(c){let l=[];for(let u in c){let f=c[u];delete f.metadata,l.push(f)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new B(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),ms=class extends rr{constructor(){super(),this.isGroup=!0,this.type="Group"}},rC={type:"move"},fo=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new ms,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new ms,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new B,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new B),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new ms,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new B,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new B,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let x of e.hand.values()){let m=t.getJointPose(x,i),p=this._getHandJoint(l,x);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],f=l.joints["thumb-tip"],d=u.position.distanceTo(f.position),h=.02,g=.005;l.inputState.pinching&&d>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&d<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(rC)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new ms;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},Ix={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},_r={h:0,s:0,l:0},Hl={h:0,s:0,l:0};function qp(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var it=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=On){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,Je.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=Je.workingColorSpace){return this.r=e,this.g=t,this.b=i,Je.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=Je.workingColorSpace){if(e=Xw(e,1),t=Ke(t,0,1),i=Ke(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=qp(o,s,e+1/3),this.g=qp(o,s,e),this.b=qp(o,s,e-1/3)}return Je.colorSpaceToWorking(this,r),this}setStyle(e,t=On){function i(s){s!==void 0&&parseFloat(s)<1&&Ae("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:Ae("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);Ae("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=On){let i=Ix[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Ae("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Ki(e.r),this.g=Ki(e.g),this.b=Ki(e.b),this}copyLinearToSRGB(e){return this.r=co(e.r),this.g=co(e.g),this.b=co(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=On){return Je.workingToColorSpace(un.copy(this),e),Math.round(Ke(un.r*255,0,255))*65536+Math.round(Ke(un.g*255,0,255))*256+Math.round(Ke(un.b*255,0,255))}getHexString(e=On){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=Je.workingColorSpace){Je.workingToColorSpace(un.copy(this),t);let i=un.r,r=un.g,s=un.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let f=o-a;switch(l=u<=.5?f/(o+a):f/(2-o-a),o){case i:c=(r-s)/f+(r<s?6:0);break;case r:c=(s-i)/f+2;break;case s:c=(i-r)/f+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=Je.workingColorSpace){return Je.workingToColorSpace(un.copy(this),t),e.r=un.r,e.g=un.g,e.b=un.b,e}getStyle(e=On){Je.workingToColorSpace(un.copy(this),e);let t=un.r,i=un.g,r=un.b;return e!==On?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(_r),this.setHSL(_r.h+e,_r.s+t,_r.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(_r),e.getHSL(Hl);let i=Hp(_r.h,Hl.h,t),r=Hp(_r.s,Hl.s,t),s=Hp(_r.l,Hl.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},un=new it;it.NAMES=Ix;var Ca=class extends rr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Ta,this.environmentIntensity=1,this.environmentRotation=new Ta,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},fi=new B,Xi=new B,Xp=new B,Yi=new B,eo=new B,to=new B,P_=new B,Yp=new B,Zp=new B,Jp=new B,Kp=new Ft,Qp=new Ft,em=new Ft,Sr=class n{constructor(e=new B,t=new B,i=new B){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),fi.subVectors(e,t),r.cross(fi);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){fi.subVectors(r,t),Xi.subVectors(i,t),Xp.subVectors(e,t);let o=fi.dot(fi),a=fi.dot(Xi),c=fi.dot(Xp),l=Xi.dot(Xi),u=Xi.dot(Xp),f=o*l-a*a;if(f===0)return s.set(0,0,0),null;let d=1/f,h=(l*c-a*u)*d,g=(o*u-a*c)*d;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Yi)===null?!1:Yi.x>=0&&Yi.y>=0&&Yi.x+Yi.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,Yi)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,Yi.x),c.addScaledVector(o,Yi.y),c.addScaledVector(a,Yi.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Kp.setScalar(0),Qp.setScalar(0),em.setScalar(0),Kp.fromBufferAttribute(e,t),Qp.fromBufferAttribute(e,i),em.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Kp,s.x),o.addScaledVector(Qp,s.y),o.addScaledVector(em,s.z),o}static isFrontFacing(e,t,i,r){return fi.subVectors(i,t),Xi.subVectors(e,t),fi.cross(Xi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return fi.subVectors(this.c,this.b),Xi.subVectors(this.a,this.b),fi.cross(Xi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;eo.subVectors(r,i),to.subVectors(s,i),Yp.subVectors(e,i);let c=eo.dot(Yp),l=to.dot(Yp);if(c<=0&&l<=0)return t.copy(i);Zp.subVectors(e,r);let u=eo.dot(Zp),f=to.dot(Zp);if(u>=0&&f<=u)return t.copy(r);let d=c*f-u*l;if(d<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(eo,o);Jp.subVectors(e,s);let h=eo.dot(Jp),g=to.dot(Jp);if(g>=0&&h<=g)return t.copy(s);let x=h*l-c*g;if(x<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(to,a);let m=u*g-h*f;if(m<=0&&f-u>=0&&h-g>=0)return P_.subVectors(s,r),a=(f-u)/(f-u+(h-g)),t.copy(r).addScaledVector(P_,a);let p=1/(m+x+d);return o=x*p,a=d*p,t.copy(i).addScaledVector(eo,o).addScaledVector(to,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},wr=class{constructor(e=new B(1/0,1/0,1/0),t=new B(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(hi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(hi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=hi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,hi):hi.fromBufferAttribute(s,o),hi.applyMatrix4(e.matrixWorld),this.expandByPoint(hi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),zl.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),zl.copy(i.boundingBox)),zl.applyMatrix4(e.matrixWorld),this.union(zl)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,hi),hi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(ma),Gl.subVectors(this.max,ma),no.subVectors(e.a,ma),io.subVectors(e.b,ma),ro.subVectors(e.c,ma),xr.subVectors(io,no),Er.subVectors(ro,io),us.subVectors(no,ro);let t=[0,-xr.z,xr.y,0,-Er.z,Er.y,0,-us.z,us.y,xr.z,0,-xr.x,Er.z,0,-Er.x,us.z,0,-us.x,-xr.y,xr.x,0,-Er.y,Er.x,0,-us.y,us.x,0];return!tm(t,no,io,ro,Gl)||(t=[1,0,0,0,1,0,0,0,1],!tm(t,no,io,ro,Gl))?!1:(Wl.crossVectors(xr,Er),t=[Wl.x,Wl.y,Wl.z],tm(t,no,io,ro,Gl))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,hi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(hi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Zi[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Zi[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Zi[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Zi[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Zi[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Zi[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Zi[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Zi[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Zi),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Zi=[new B,new B,new B,new B,new B,new B,new B,new B],hi=new B,zl=new wr,no=new B,io=new B,ro=new B,xr=new B,Er=new B,us=new B,ma=new B,Gl=new B,Wl=new B,ds=new B;function tm(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){ds.fromArray(n,s);let a=r.x*Math.abs(ds.x)+r.y*Math.abs(ds.y)+r.z*Math.abs(ds.z),c=e.dot(ds),l=t.dot(ds),u=i.dot(ds);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Gt=new B,jl=new ut,sC=0,fn=class extends Di{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:sC++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=gm,this.updateRanges=[],this.gpuType=vi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)jl.fromBufferAttribute(this,t),jl.applyMatrix3(e),this.setXY(t,jl.x,jl.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix3(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix4(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyNormalMatrix(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.transformDirection(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=ha(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Sn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=ha(t,this.array)),t}setX(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=ha(t,this.array)),t}setY(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=ha(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=ha(t,this.array)),t}setW(e,t){return this.normalized&&(t=Sn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array),r=Sn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=Sn(t,this.array),i=Sn(i,this.array),r=Sn(r,this.array),s=Sn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==gm&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var Ia=class extends fn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Aa=class extends fn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Tn=class extends fn{constructor(e,t,i){super(new Float32Array(e),t,i)}},oC=new wr,ga=new B,nm=new B,Cr=class{constructor(e=new B,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):oC.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;ga.subVectors(e,this.center);let t=ga.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(ga,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(nm.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(ga.copy(e.center).add(nm)),this.expandByPoint(ga.copy(e.center).sub(nm))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},aC=0,Jn=new Ot,im=new rr,so=new B,Ln=new wr,va=new wr,Zt=new B,_n=class n extends Di{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:aC++}),this.uuid=Ya(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new($w(e)?Aa:Ia)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Ue().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Jn.makeRotationFromQuaternion(e),this.applyMatrix4(Jn),this}rotateX(e){return Jn.makeRotationX(e),this.applyMatrix4(Jn),this}rotateY(e){return Jn.makeRotationY(e),this.applyMatrix4(Jn),this}rotateZ(e){return Jn.makeRotationZ(e),this.applyMatrix4(Jn),this}translate(e,t,i){return Jn.makeTranslation(e,t,i),this.applyMatrix4(Jn),this}scale(e,t,i){return Jn.makeScale(e,t,i),this.applyMatrix4(Jn),this}lookAt(e){return im.lookAt(e),im.updateMatrix(),this.applyMatrix4(im.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(so).negate(),this.translate(so.x,so.y,so.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Tn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&Ae("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new wr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new B(-1/0,-1/0,-1/0),new B(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Ln.setFromBufferAttribute(s),this.morphTargetsRelative?(Zt.addVectors(this.boundingBox.min,Ln.min),this.boundingBox.expandByPoint(Zt),Zt.addVectors(this.boundingBox.max,Ln.max),this.boundingBox.expandByPoint(Zt)):(this.boundingBox.expandByPoint(Ln.min),this.boundingBox.expandByPoint(Ln.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Ne('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Cr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Ne("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new B,1/0);return}if(e){let i=this.boundingSphere.center;if(Ln.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];va.setFromBufferAttribute(a),this.morphTargetsRelative?(Zt.addVectors(Ln.min,va.min),Ln.expandByPoint(Zt),Zt.addVectors(Ln.max,va.max),Ln.expandByPoint(Zt)):(Ln.expandByPoint(va.min),Ln.expandByPoint(va.max))}Ln.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Zt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Zt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Zt.fromBufferAttribute(a,l),c&&(so.fromBufferAttribute(e,l),Zt.add(so)),r=Math.max(r,i.distanceToSquared(Zt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Ne('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Ne("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new fn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let y=0;y<i.count;y++)a[y]=new B,c[y]=new B;let l=new B,u=new B,f=new B,d=new ut,h=new ut,g=new ut,x=new B,m=new B;function p(y,w,O){l.fromBufferAttribute(i,y),u.fromBufferAttribute(i,w),f.fromBufferAttribute(i,O),d.fromBufferAttribute(s,y),h.fromBufferAttribute(s,w),g.fromBufferAttribute(s,O),u.sub(l),f.sub(l),h.sub(d),g.sub(d);let C=1/(h.x*g.y-g.x*h.y);isFinite(C)&&(x.copy(u).multiplyScalar(g.y).addScaledVector(f,-h.y).multiplyScalar(C),m.copy(f).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(C),a[y].add(x),a[w].add(x),a[O].add(x),c[y].add(m),c[w].add(m),c[O].add(m))}let b=this.groups;b.length===0&&(b=[{start:0,count:e.count}]);for(let y=0,w=b.length;y<w;++y){let O=b[y],C=O.start,L=O.count;for(let $=C,X=C+L;$<X;$+=3)p(e.getX($+0),e.getX($+1),e.getX($+2))}let M=new B,S=new B,D=new B,T=new B;function I(y){D.fromBufferAttribute(r,y),T.copy(D);let w=a[y];M.copy(w),M.sub(D.multiplyScalar(D.dot(w))).normalize(),S.crossVectors(T,w);let C=S.dot(c[y])<0?-1:1;o.setXYZW(y,M.x,M.y,M.z,C)}for(let y=0,w=b.length;y<w;++y){let O=b[y],C=O.start,L=O.count;for(let $=C,X=C+L;$<X;$+=3)I(e.getX($+0)),I(e.getX($+1)),I(e.getX($+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new fn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let d=0,h=i.count;d<h;d++)i.setXYZ(d,0,0,0);let r=new B,s=new B,o=new B,a=new B,c=new B,l=new B,u=new B,f=new B;if(e)for(let d=0,h=e.count;d<h;d+=3){let g=e.getX(d+0),x=e.getX(d+1),m=e.getX(d+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,x),o.fromBufferAttribute(t,m),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,x),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(x,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let d=0,h=t.count;d<h;d+=3)r.fromBufferAttribute(t,d+0),s.fromBufferAttribute(t,d+1),o.fromBufferAttribute(t,d+2),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),i.setXYZ(d+0,u.x,u.y,u.z),i.setXYZ(d+1,u.x,u.y,u.z),i.setXYZ(d+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Zt.fromBufferAttribute(e,t),Zt.normalize(),e.setXYZ(t,Zt.x,Zt.y,Zt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,f=a.normalized,d=new l.constructor(c.length*u),h=0,g=0;for(let x=0,m=c.length;x<m;x++){a.isInterleavedBufferAttribute?h=c[x]*a.data.stride+a.offset:h=c[x]*u;for(let p=0;p<u;p++)d[g++]=l[h++]}return new fn(d,u,f)}if(this.index===null)return Ae("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,f=l.length;u<f;u++){let d=l[u],h=e(d,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let f=0,d=l.length;f<d;f++){let h=l[f];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],f=s[l];for(let d=0,h=f.length;d<h;d++)u.push(f[d].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let f=o[l];this.addGroup(f.start,f.count,f.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var cC=0,er=class extends Di{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:cC++}),this.uuid=Ya(),this.name="",this.type="Material",this.blending=gs,this.side=Qi,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=uu,this.blendDst=du,this.blendEquation=Tr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new it(0,0,0),this.blendAlpha=0,this.depthFunc=vs,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=mm,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=ps,this.stencilZFail=ps,this.stencilZPass=ps,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Ae(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Ae(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==gs&&(i.blending=this.blending),this.side!==Qi&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==uu&&(i.blendSrc=this.blendSrc),this.blendDst!==du&&(i.blendDst=this.blendDst),this.blendEquation!==Tr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==vs&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==mm&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==ps&&(i.stencilFail=this.stencilFail),this.stencilZFail!==ps&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==ps&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var Ji=new B,rm=new B,$l=new B,br=new B,sm=new B,ql=new B,om=new B,ho=class{constructor(e=new B,t=new B(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Ji)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Ji.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Ji.copy(this.origin).addScaledVector(this.direction,t),Ji.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){rm.copy(e).add(t).multiplyScalar(.5),$l.copy(t).sub(e).normalize(),br.copy(this.origin).sub(rm);let s=e.distanceTo(t)*.5,o=-this.direction.dot($l),a=br.dot(this.direction),c=-br.dot($l),l=br.lengthSq(),u=Math.abs(1-o*o),f,d,h,g;if(u>0)if(f=o*c-a,d=o*a-c,g=s*u,f>=0)if(d>=-g)if(d<=g){let x=1/u;f*=x,d*=x,h=f*(f+o*d+2*a)+d*(o*f+d+2*c)+l}else d=s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d=-s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d<=-g?(f=Math.max(0,-(-o*s+a)),d=f>0?-s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l):d<=g?(f=0,d=Math.min(Math.max(-s,-c),s),h=d*(d+2*c)+l):(f=Math.max(0,-(o*s+a)),d=f>0?s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l);else d=o>0?-s:s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,f),r&&r.copy(rm).addScaledVector($l,d),h}intersectSphere(e,t){Ji.subVectors(e.center,this.origin);let i=Ji.dot(this.direction),r=Ji.dot(Ji)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,f=1/this.direction.z,d=this.origin;return l>=0?(i=(e.min.x-d.x)*l,r=(e.max.x-d.x)*l):(i=(e.max.x-d.x)*l,r=(e.min.x-d.x)*l),u>=0?(s=(e.min.y-d.y)*u,o=(e.max.y-d.y)*u):(s=(e.max.y-d.y)*u,o=(e.min.y-d.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),f>=0?(a=(e.min.z-d.z)*f,c=(e.max.z-d.z)*f):(a=(e.max.z-d.z)*f,c=(e.min.z-d.z)*f),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Ji)!==null}intersectTriangle(e,t,i,r,s){sm.subVectors(t,e),ql.subVectors(i,e),om.crossVectors(sm,ql);let o=this.direction.dot(om),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;br.subVectors(this.origin,e);let c=a*this.direction.dot(ql.crossVectors(br,ql));if(c<0)return null;let l=a*this.direction.dot(sm.cross(br));if(l<0||c+l>o)return null;let u=-a*br.dot(om);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Da=class extends er{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new it(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Ta,this.combine=Sm,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},L_=new Ot,fs=new ho,Xl=new Cr,O_=new B,Yl=new B,Zl=new B,Jl=new B,am=new B,Kl=new B,F_=new B,Ql=new B,Kn=class extends rr{constructor(e=new _n,t=new Da){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Kl.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],f=s[c];u!==0&&(am.fromBufferAttribute(f,e),o?Kl.addScaledVector(am,u):Kl.addScaledVector(am.sub(t),u))}t.add(Kl)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Xl.copy(i.boundingSphere),Xl.applyMatrix4(s),fs.copy(e.ray).recast(e.near),!(Xl.containsPoint(fs.origin)===!1&&(fs.intersectSphere(Xl,O_)===null||fs.origin.distanceToSquared(O_)>(e.far-e.near)**2))&&(L_.copy(s).invert(),fs.copy(e.ray).applyMatrix4(L_),!(i.boundingBox!==null&&fs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,fs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,f=s.attributes.normal,d=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],b=Math.max(m.start,h.start),M=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let S=b,D=M;S<D;S+=3){let T=a.getX(S),I=a.getX(S+1),y=a.getX(S+2);r=eu(this,p,e,i,l,u,f,T,I,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(a.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let b=a.getX(m),M=a.getX(m+1),S=a.getX(m+2);r=eu(this,o,e,i,l,u,f,b,M,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,x=d.length;g<x;g++){let m=d[g],p=o[m.materialIndex],b=Math.max(m.start,h.start),M=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let S=b,D=M;S<D;S+=3){let T=S,I=S+1,y=S+2;r=eu(this,p,e,i,l,u,f,T,I,y),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),x=Math.min(c.count,h.start+h.count);for(let m=g,p=x;m<p;m+=3){let b=m,M=m+1,S=m+2;r=eu(this,o,e,i,l,u,f,b,M,S),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function lC(n,e,t,i,r,s,o,a){let c;if(e.side===xn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===Qi,a),c===null)return null;Ql.copy(a),Ql.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Ql);return l<t.near||l>t.far?null:{distance:l,point:Ql.clone(),object:n}}function eu(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Yl),n.getVertexPosition(c,Zl),n.getVertexPosition(l,Jl);let u=lC(n,e,t,i,Yl,Zl,Jl,F_);if(u){let f=new B;Sr.getBarycoord(F_,Yl,Zl,Jl,f),r&&(u.uv=Sr.getInterpolatedAttribute(r,a,c,l,f,new ut)),s&&(u.uv1=Sr.getInterpolatedAttribute(s,a,c,l,f,new ut)),o&&(u.normal=Sr.getInterpolatedAttribute(o,a,c,l,f,new B),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let d={a,b:c,c:l,normal:new B,materialIndex:0};Sr.getNormal(Yl,Zl,Jl,d.normal),u.face=d,u.barycoord=f}return u}var Cu=class extends ir{constructor(e=null,t=1,i=1,r,s,o,a,c,l=Jt,u=Jt,f,d){super(null,o,a,c,l,u,r,s,f,d),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var cm=new B,uC=new B,dC=new Ue,Ci=class{constructor(e=new B(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=cm.subVectors(i,t).cross(uC.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(cm),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||dC.getNormalMatrix(e),r=this.coplanarPoint(cm).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},hs=new Cr,fC=new ut(.5,.5),tu=new B,Ra=class{constructor(e=new Ci,t=new Ci,i=new Ci,r=new Ci,s=new Ci,o=new Ci){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=pi,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],f=s[5],d=s[6],h=s[7],g=s[8],x=s[9],m=s[10],p=s[11],b=s[12],M=s[13],S=s[14],D=s[15];if(r[0].setComponents(l-o,h-u,p-g,D-b).normalize(),r[1].setComponents(l+o,h+u,p+g,D+b).normalize(),r[2].setComponents(l+a,h+f,p+x,D+M).normalize(),r[3].setComponents(l-a,h-f,p-x,D-M).normalize(),i)r[4].setComponents(c,d,m,S).normalize(),r[5].setComponents(l-c,h-d,p-m,D-S).normalize();else if(r[4].setComponents(l-c,h-d,p-m,D-S).normalize(),t===pi)r[5].setComponents(l+c,h+d,p+m,D+S).normalize();else if(t===ba)r[5].setComponents(c,d,m,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),hs.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),hs.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(hs)}intersectsSprite(e){hs.center.set(0,0,0);let t=fC.distanceTo(e.center);return hs.radius=.7071067811865476+t,hs.applyMatrix4(e.matrixWorld),this.intersectsSphere(hs)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(tu.x=r.normal.x>0?e.max.x:e.min.x,tu.y=r.normal.y>0?e.max.y:e.min.y,tu.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(tu)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var po=class extends er{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new it(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Iu=new B,Au=new B,k_=new Ot,ya=new ho,nu=new Cr,lm=new B,U_=new B,Du=class extends rr{constructor(e=new _n,t=new po){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)Iu.fromBufferAttribute(t,r-1),Au.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Iu.distanceTo(Au);e.setAttribute("lineDistance",new Tn(i,1))}else Ae("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),nu.copy(i.boundingSphere),nu.applyMatrix4(r),nu.radius+=s,e.ray.intersectsSphere(nu)===!1)return;k_.copy(r).invert(),ya.copy(e.ray).applyMatrix4(k_);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,d=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let x=h,m=g-1;x<m;x+=l){let p=u.getX(x),b=u.getX(x+1),M=iu(this,e,ya,c,p,b,x);M&&t.push(M)}if(this.isLineLoop){let x=u.getX(g-1),m=u.getX(h),p=iu(this,e,ya,c,x,m,g-1);p&&t.push(p)}}else{let h=Math.max(0,o.start),g=Math.min(d.count,o.start+o.count);for(let x=h,m=g-1;x<m;x+=l){let p=iu(this,e,ya,c,x,x+1,x);p&&t.push(p)}if(this.isLineLoop){let x=iu(this,e,ya,c,g-1,h,g-1);x&&t.push(x)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function iu(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(Iu.fromBufferAttribute(a,r),Au.fromBufferAttribute(a,s),t.distanceSqToSegment(Iu,Au,lm,U_)>i)return;lm.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(lm);if(!(l<e.near||l>e.far))return{distance:l,point:U_.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var B_=new B,V_=new B,Na=class extends Du{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)B_.fromBufferAttribute(t,r),V_.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+B_.distanceTo(V_);e.setAttribute("lineDistance",new Tn(i,1))}else Ae("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var mo=class extends er{constructor(e){super(),this.isPointsMaterial=!0,this.type="PointsMaterial",this.color=new it(16777215),this.map=null,this.alphaMap=null,this.size=1,this.sizeAttenuation=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.size=e.size,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},H_=new Ot,vm=new ho,ru=new Cr,su=new B,Pa=class extends rr{constructor(e=new _n,t=new mo){super(),this.isPoints=!0,this.type="Points",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Points.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),ru.copy(i.boundingSphere),ru.applyMatrix4(r),ru.radius+=s,e.ray.intersectsSphere(ru)===!1)return;H_.copy(r).invert(),vm.copy(e.ray).applyMatrix4(H_);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=i.index,f=i.attributes.position;if(l!==null){let d=Math.max(0,o.start),h=Math.min(l.count,o.start+o.count);for(let g=d,x=h;g<x;g++){let m=l.getX(g);su.fromBufferAttribute(f,m),z_(su,m,c,r,e,t,this)}}else{let d=Math.max(0,o.start),h=Math.min(f.count,o.start+o.count);for(let g=d,x=h;g<x;g++)su.fromBufferAttribute(f,g),z_(su,g,c,r,e,t,this)}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function z_(n,e,t,i,r,s,o){let a=vm.distanceSqToPoint(n);if(a<t){let c=new B;vm.closestPointToPoint(n,c),c.applyMatrix4(i);let l=r.ray.origin.distanceTo(c);if(l<r.near||l>r.far)return;s.push({distance:l,distanceToRay:Math.sqrt(a),point:c,index:e,face:null,faceIndex:null,barycoord:null,object:o})}}var La=class extends ir{constructor(e=[],t=Rr,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var tr=class extends ir{constructor(e,t,i=gi,r,s,o,a=Jt,c=Jt,l,u=Ai,f=1){if(u!==Ai&&u!==Pr)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let d={width:e,height:t,depth:f};super(d,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new uo(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Ru=class extends tr{constructor(e,t=gi,i=Rr,r,s,o=Jt,a=Jt,c,l=Ai){let u={width:e,height:e,depth:1},f=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=f,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Oa=class extends ir{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},go=class n extends _n{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],f=[],d=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new Tn(l,3)),this.setAttribute("normal",new Tn(u,3)),this.setAttribute("uv",new Tn(f,2));function g(x,m,p,b,M,S,D,T,I,y,w){let O=S/I,C=D/y,L=S/2,$=D/2,X=T/2,P=I+1,W=y+1,k=0,te=0,ne=new B;for(let he=0;he<W;he++){let be=he*C-$;for(let we=0;we<P;we++){let Qe=we*O-L;ne[x]=Qe*b,ne[m]=be*M,ne[p]=X,l.push(ne.x,ne.y,ne.z),ne[x]=0,ne[m]=0,ne[p]=T>0?1:-1,u.push(ne.x,ne.y,ne.z),f.push(we/I),f.push(1-he/y),k+=1}}for(let he=0;he<y;he++)for(let be=0;be<I;be++){let we=d+be+P*he,Qe=d+be+P*(he+1),dt=d+(be+1)+P*(he+1),ze=d+(be+1)+P*he;c.push(we,Qe,ze),c.push(Qe,dt,ze),te+=6}a.addGroup(h,te,w),h+=te,d+=k}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Fa=class n extends _n{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,f=e/a,d=t/c,h=[],g=[],x=[],m=[];for(let p=0;p<u;p++){let b=p*d-o;for(let M=0;M<l;M++){let S=M*f-s;g.push(S,-b,0),x.push(0,0,1),m.push(M/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let b=0;b<a;b++){let M=b+l*p,S=b+l*(p+1),D=b+1+l*(p+1),T=b+1+l*p;h.push(M,S,T),h.push(S,D,T)}this.setIndex(h),this.setAttribute("position",new Tn(g,3)),this.setAttribute("normal",new Tn(x,3)),this.setAttribute("uv",new Tn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function _s(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(G_(r))r.isRenderTargetTexture?(Ae("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(G_(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function hn(n){let e={};for(let t=0;t<n.length;t++){let i=_s(n[t]);for(let r in i)e[r]=i[r]}return e}function G_(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function hC(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function zm(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:Je.workingColorSpace}var Ax={clone:_s,merge:hn},pC=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,mC=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,kn=class extends er{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=pC,this.fragmentShader=mC,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=_s(e.uniforms),this.uniformsGroups=hC(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Nu=class extends kn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var Pu=class extends er{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=gx,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Lu=class extends er{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function ou(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var Ir=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Ou=class extends Ir{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:fm,endingEnd:fm}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case hm:s=e,a=2*t-i;break;case pm:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case hm:o=e,c=2*i-t;break;case pm:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,f=this._offsetNext,d=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),x=g*g,m=x*g,p=-d*m+2*d*x-d*g,b=(1+d)*m+(-1.5-2*d)*x+(-.5+d)*g+1,M=(-1-h)*m+(1.5+h)*x+.5*g,S=h*m-h*x;for(let D=0;D!==a;++D)s[D]=p*o[u+D]+b*o[l+D]+M*o[c+D]+S*o[f+D];return s}},Fu=class extends Ir{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),f=1-u;for(let d=0;d!==a;++d)s[d]=o[l+d]*f+o[c+d]*u;return s}},ku=class extends Ir{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Uu=class extends Ir{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,f=u.inTangents,d=u.outTangents;if(!f||!d){let x=(i-t)/(r-t),m=1-x;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*x;return s}let h=a*2,g=e-1;for(let x=0;x!==a;++x){let m=o[l+x],p=o[c+x],b=g*h+x*2,M=d[b],S=d[b+1],D=e*h+x*2,T=f[D],I=f[D+1],y=(i-t)/(r-t),w,O,C,L,$;for(let X=0;X<8;X++){w=y*y,O=w*y,C=1-y,L=C*C,$=L*C;let W=$*t+3*L*y*M+3*C*w*T+O*r-i;if(Math.abs(W)<1e-10)break;let k=3*L*(M-t)+6*C*y*(T-M)+3*w*(r-T);if(Math.abs(k)<1e-10)break;y=y-W/k,y=Math.max(0,Math.min(1,y))}s[x]=$*m+3*L*y*S+3*C*w*I+O*p}return s}},Un=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=ou(t,this.TimeBufferType),this.values=ou(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:ou(e.times,Array),values:ou(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new ku(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new Fu(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Ou(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Uu(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case _a:t=this.InterpolantFactoryMethodDiscrete;break;case Eu:t=this.InterpolantFactoryMethodLinear;break;case lu:t=this.InterpolantFactoryMethodSmooth;break;case dm:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Ae("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return _a;case this.InterpolantFactoryMethodLinear:return Eu;case this.InterpolantFactoryMethodSmooth:return lu;case this.InterpolantFactoryMethodBezier:return dm}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Ne("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Ne("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Ne("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Ne("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&qw(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Ne("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===lu,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let f=a*i,d=f-i,h=f+i;for(let g=0;g!==i;++g){let x=t[f+g];if(x!==t[d+g]||x!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let f=a*i,d=o*i;for(let h=0;h!==i;++h)t[d+h]=t[f+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Un.prototype.ValueTypeName="";Un.prototype.TimeBufferType=Float32Array;Un.prototype.ValueBufferType=Float32Array;Un.prototype.DefaultInterpolation=Eu;var Ar=class extends Un{constructor(e,t,i){super(e,t,i)}};Ar.prototype.ValueTypeName="bool";Ar.prototype.ValueBufferType=Array;Ar.prototype.DefaultInterpolation=_a;Ar.prototype.InterpolantFactoryMethodLinear=void 0;Ar.prototype.InterpolantFactoryMethodSmooth=void 0;var Bu=class extends Un{constructor(e,t,i,r){super(e,t,i,r)}};Bu.prototype.ValueTypeName="color";var Vu=class extends Un{constructor(e,t,i,r){super(e,t,i,r)}};Vu.prototype.ValueTypeName="number";var Hu=class extends Ir{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Ri.slerpFlat(s,0,o,l-a,o,l,c);return s}},ka=class extends Un{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Hu(this.times,this.values,this.getValueSize(),e)}};ka.prototype.ValueTypeName="quaternion";ka.prototype.InterpolantFactoryMethodSmooth=void 0;var Dr=class extends Un{constructor(e,t,i){super(e,t,i)}};Dr.prototype.ValueTypeName="string";Dr.prototype.ValueBufferType=Array;Dr.prototype.DefaultInterpolation=_a;Dr.prototype.InterpolantFactoryMethodLinear=void 0;Dr.prototype.InterpolantFactoryMethodSmooth=void 0;var zu=class extends Un{constructor(e,t,i,r){super(e,t,i,r)}};zu.prototype.ValueTypeName="vector";var au=new B,cu=new Ri,wi=new B,Ua=class extends rr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Ot,this.projectionMatrix=new Ot,this.projectionMatrixInverse=new Ot,this.coordinateSystem=pi,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(au,cu,wi),wi.x===1&&wi.y===1&&wi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(au,cu,wi.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(au,cu,wi),wi.x===1&&wi.y===1&&wi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(au,cu,wi.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},Mr=new B,W_=new ut,j_=new ut,dn=class extends Ua{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Mu*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Vp*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Mu*2*Math.atan(Math.tan(Vp*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Mr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Mr.x,Mr.y).multiplyScalar(-e/Mr.z),Mr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Mr.x,Mr.y).multiplyScalar(-e/Mr.z)}getViewSize(e,t){return this.getViewBounds(e,W_,j_),t.subVectors(j_,W_)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Vp*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Ba=class extends Ua{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var oo=-90,ao=1,Gu=class extends rr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new dn(oo,ao,e,t);r.layers=this.layers,this.add(r);let s=new dn(oo,ao,e,t);s.layers=this.layers,this.add(s);let o=new dn(oo,ao,e,t);o.layers=this.layers,this.add(o);let a=new dn(oo,ao,e,t);a.layers=this.layers,this.add(a);let c=new dn(oo,ao,e,t);c.layers=this.layers,this.add(c);let l=new dn(oo,ao,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===pi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===ba)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,f=e.getRenderTarget(),d=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let x=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=x,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(f,d,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Wu=class extends dn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var Gm="\\[\\]\\.:\\/",gC=new RegExp("["+Gm+"]","g"),Wm="[^"+Gm+"]",vC="[^"+Gm.replace("\\.","")+"]",yC=/((?:WC+[\/:])*)/.source.replace("WC",Wm),_C=/(WCOD+)?/.source.replace("WCOD",vC),xC=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Wm),EC=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Wm),bC=new RegExp("^"+yC+_C+xC+EC+"$"),MC=["material","materials","bones","map"],ym=class{constructor(e,t,i){let r=i||Lt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Lt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(gC,"")}static parseTrackName(t){let i=bC.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);MC.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Ae("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Ne("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Ne("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let f=0;f<t.length;f++)if(t[f].name===u){u=f;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Ne("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Ne("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Ne("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Ne("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Ne("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Ne("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=ym,n})();Lt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Lt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Lt.prototype.GetterByBindingType=[Lt.prototype._getValue_direct,Lt.prototype._getValue_array,Lt.prototype._getValue_arrayElement,Lt.prototype._getValue_toArray];Lt.prototype.SetterByBindingTypeAndVersioning=[[Lt.prototype._setValue_direct,Lt.prototype._setValue_direct_setNeedsUpdate,Lt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Lt.prototype._setValue_array,Lt.prototype._setValue_array_setNeedsUpdate,Lt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Lt.prototype._setValue_arrayElement,Lt.prototype._setValue_arrayElement_setNeedsUpdate,Lt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Lt.prototype._setValue_fromArray,Lt.prototype._setValue_fromArray_setNeedsUpdate,Lt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var s6=new Float32Array(1);var _m=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};function jm(n,e,t,i){let r=SC(i);switch(t){case Fm:return n*e;case Um:return n*e/r.components*r.byteLength;case Ju:return n*e/r.components*r.byteLength;case Lr:return n*e*2/r.components*r.byteLength;case Ku:return n*e*2/r.components*r.byteLength;case km:return n*e*3/r.components*r.byteLength;case Qn:return n*e*4/r.components*r.byteLength;case Qu:return n*e*4/r.components*r.byteLength;case Ga:case Wa:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case ja:case $a:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case td:case id:return Math.max(n,16)*Math.max(e,8)/4;case ed:case nd:return Math.max(n,8)*Math.max(e,8)/2;case rd:case sd:case ad:case cd:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case od:case qa:case ld:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ud:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case dd:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case fd:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case hd:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case pd:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case md:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case gd:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case vd:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case yd:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case _d:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case xd:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Ed:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case bd:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Md:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Sd:case Td:case wd:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Cd:case Id:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Xa:case Ad:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function SC(n){switch(n){case Bn:case Nm:return{byteLength:1,components:1};case yo:case Pm:case Li:return{byteLength:2,components:1};case Yu:case Zu:return{byteLength:2,components:4};case gi:case Xu:case vi:return{byteLength:4,components:1};case Lm:case Om:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?Ae("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function Kx(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function wC(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,f=l.byteLength,d=n.createBuffer();n.bindBuffer(c,d),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:d,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:f}}function i(a,c,l){let u=c.array,f=c.updateRanges;if(n.bindBuffer(l,a),f.length===0)n.bufferSubData(l,0,u);else{f.sort((h,g)=>h.start-g.start);let d=0;for(let h=1;h<f.length;h++){let g=f[d],x=f[h];x.start<=g.start+g.count+1?g.count=Math.max(g.count,x.start+x.count-g.start):(++d,f[d]=x)}f.length=d+1;for(let h=0,g=f.length;h<g;h++){let x=f[h];n.bufferSubData(l,x.start*u.BYTES_PER_ELEMENT,u,x.start,x.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var CC=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,IC=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,AC=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,DC=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,RC=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,NC=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,PC=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,LC=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,OC=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,FC=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,kC=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,UC=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,BC=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,VC=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,HC=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,zC=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,GC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,WC=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,jC=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,$C=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,qC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,XC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,YC=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,ZC=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,JC=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,KC=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,QC=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,eI=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,tI=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,nI=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,iI="gl_FragColor = linearToOutputTexel( gl_FragColor );",rI=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,sI=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,oI=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,aI=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,cI=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,lI=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,uI=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,dI=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,fI=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,hI=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,pI=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,mI=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,gI=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,vI=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,yI=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,_I=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,xI=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,EI=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,bI=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,MI=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,SI=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,TI=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,wI=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = inverseTransformDirection( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,CI=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,II=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,AI=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,DI=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,RI=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,NI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,PI=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,LI=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,OI=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,FI=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,kI=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,UI=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,BI=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,VI=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,HI=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,zI=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,GI=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,WI=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,jI=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,$I=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,qI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,XI=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,YI=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,ZI=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,JI=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,KI=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,QI=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,eA=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,tA=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,nA=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,iA=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,rA=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,sA=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,oA=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,aA=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,cA=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,lA=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,uA=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,dA=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,fA=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,hA=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,pA=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,mA=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,gA=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,vA=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,yA=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,_A=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,xA=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,EA=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,bA=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,MA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,SA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,TA=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,wA=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,CA=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,IA=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,AA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,DA=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,RA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,NA=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,PA=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,LA=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,OA=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,FA=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,kA=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,UA=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,BA=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,VA=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,HA=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,zA=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,GA=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,WA=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,jA=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,$A=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,qA=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,XA=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,YA=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,ZA=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,JA=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,KA=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,QA=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,e1=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,t1=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,n1=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,i1=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,r1=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,s1=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,o1=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,We={alphahash_fragment:CC,alphahash_pars_fragment:IC,alphamap_fragment:AC,alphamap_pars_fragment:DC,alphatest_fragment:RC,alphatest_pars_fragment:NC,aomap_fragment:PC,aomap_pars_fragment:LC,batching_pars_vertex:OC,batching_vertex:FC,begin_vertex:kC,beginnormal_vertex:UC,bsdfs:BC,iridescence_fragment:VC,bumpmap_pars_fragment:HC,clipping_planes_fragment:zC,clipping_planes_pars_fragment:GC,clipping_planes_pars_vertex:WC,clipping_planes_vertex:jC,color_fragment:$C,color_pars_fragment:qC,color_pars_vertex:XC,color_vertex:YC,common:ZC,cube_uv_reflection_fragment:JC,defaultnormal_vertex:KC,displacementmap_pars_vertex:QC,displacementmap_vertex:eI,emissivemap_fragment:tI,emissivemap_pars_fragment:nI,colorspace_fragment:iI,colorspace_pars_fragment:rI,envmap_fragment:sI,envmap_common_pars_fragment:oI,envmap_pars_fragment:aI,envmap_pars_vertex:cI,envmap_physical_pars_fragment:_I,envmap_vertex:lI,fog_vertex:uI,fog_pars_vertex:dI,fog_fragment:fI,fog_pars_fragment:hI,gradientmap_pars_fragment:pI,lightmap_pars_fragment:mI,lights_lambert_fragment:gI,lights_lambert_pars_fragment:vI,lights_pars_begin:yI,lights_toon_fragment:xI,lights_toon_pars_fragment:EI,lights_phong_fragment:bI,lights_phong_pars_fragment:MI,lights_physical_fragment:SI,lights_physical_pars_fragment:TI,lights_fragment_begin:wI,lights_fragment_maps:CI,lights_fragment_end:II,lightprobes_pars_fragment:AI,logdepthbuf_fragment:DI,logdepthbuf_pars_fragment:RI,logdepthbuf_pars_vertex:NI,logdepthbuf_vertex:PI,map_fragment:LI,map_pars_fragment:OI,map_particle_fragment:FI,map_particle_pars_fragment:kI,metalnessmap_fragment:UI,metalnessmap_pars_fragment:BI,morphinstance_vertex:VI,morphcolor_vertex:HI,morphnormal_vertex:zI,morphtarget_pars_vertex:GI,morphtarget_vertex:WI,normal_fragment_begin:jI,normal_fragment_maps:$I,normal_pars_fragment:qI,normal_pars_vertex:XI,normal_vertex:YI,normalmap_pars_fragment:ZI,clearcoat_normal_fragment_begin:JI,clearcoat_normal_fragment_maps:KI,clearcoat_pars_fragment:QI,iridescence_pars_fragment:eA,opaque_fragment:tA,packing:nA,premultiplied_alpha_fragment:iA,project_vertex:rA,dithering_fragment:sA,dithering_pars_fragment:oA,roughnessmap_fragment:aA,roughnessmap_pars_fragment:cA,shadowmap_pars_fragment:lA,shadowmap_pars_vertex:uA,shadowmap_vertex:dA,shadowmask_pars_fragment:fA,skinbase_vertex:hA,skinning_pars_vertex:pA,skinning_vertex:mA,skinnormal_vertex:gA,specularmap_fragment:vA,specularmap_pars_fragment:yA,tonemapping_fragment:_A,tonemapping_pars_fragment:xA,transmission_fragment:EA,transmission_pars_fragment:bA,uv_pars_fragment:MA,uv_pars_vertex:SA,uv_vertex:TA,worldpos_vertex:wA,background_vert:CA,background_frag:IA,backgroundCube_vert:AA,backgroundCube_frag:DA,cube_vert:RA,cube_frag:NA,depth_vert:PA,depth_frag:LA,distance_vert:OA,distance_frag:FA,equirect_vert:kA,equirect_frag:UA,linedashed_vert:BA,linedashed_frag:VA,meshbasic_vert:HA,meshbasic_frag:zA,meshlambert_vert:GA,meshlambert_frag:WA,meshmatcap_vert:jA,meshmatcap_frag:$A,meshnormal_vert:qA,meshnormal_frag:XA,meshphong_vert:YA,meshphong_frag:ZA,meshphysical_vert:JA,meshphysical_frag:KA,meshtoon_vert:QA,meshtoon_frag:e1,points_vert:t1,points_frag:n1,shadow_vert:i1,shadow_frag:r1,sprite_vert:s1,sprite_frag:o1},fe={common:{diffuse:{value:new it(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Ue},alphaMap:{value:null},alphaMapTransform:{value:new Ue},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Ue}},envmap:{envMap:{value:null},envMapRotation:{value:new Ue},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Ue}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Ue}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Ue},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Ue},normalScale:{value:new ut(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Ue},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Ue}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Ue}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Ue}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new it(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new B},probesMax:{value:new B},probesResolution:{value:new B}},points:{diffuse:{value:new it(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Ue},alphaTest:{value:0},uvTransform:{value:new Ue}},sprite:{diffuse:{value:new it(16777215)},opacity:{value:1},center:{value:new ut(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Ue},alphaMap:{value:null},alphaMapTransform:{value:new Ue},alphaTest:{value:0}}},Fi={basic:{uniforms:hn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.fog]),vertexShader:We.meshbasic_vert,fragmentShader:We.meshbasic_frag},lambert:{uniforms:hn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,fe.lights,{emissive:{value:new it(0)},envMapIntensity:{value:1}}]),vertexShader:We.meshlambert_vert,fragmentShader:We.meshlambert_frag},phong:{uniforms:hn([fe.common,fe.specularmap,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,fe.lights,{emissive:{value:new it(0)},specular:{value:new it(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:We.meshphong_vert,fragmentShader:We.meshphong_frag},standard:{uniforms:hn([fe.common,fe.envmap,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.roughnessmap,fe.metalnessmap,fe.fog,fe.lights,{emissive:{value:new it(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:We.meshphysical_vert,fragmentShader:We.meshphysical_frag},toon:{uniforms:hn([fe.common,fe.aomap,fe.lightmap,fe.emissivemap,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.gradientmap,fe.fog,fe.lights,{emissive:{value:new it(0)}}]),vertexShader:We.meshtoon_vert,fragmentShader:We.meshtoon_frag},matcap:{uniforms:hn([fe.common,fe.bumpmap,fe.normalmap,fe.displacementmap,fe.fog,{matcap:{value:null}}]),vertexShader:We.meshmatcap_vert,fragmentShader:We.meshmatcap_frag},points:{uniforms:hn([fe.points,fe.fog]),vertexShader:We.points_vert,fragmentShader:We.points_frag},dashed:{uniforms:hn([fe.common,fe.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:We.linedashed_vert,fragmentShader:We.linedashed_frag},depth:{uniforms:hn([fe.common,fe.displacementmap]),vertexShader:We.depth_vert,fragmentShader:We.depth_frag},normal:{uniforms:hn([fe.common,fe.bumpmap,fe.normalmap,fe.displacementmap,{opacity:{value:1}}]),vertexShader:We.meshnormal_vert,fragmentShader:We.meshnormal_frag},sprite:{uniforms:hn([fe.sprite,fe.fog]),vertexShader:We.sprite_vert,fragmentShader:We.sprite_frag},background:{uniforms:{uvTransform:{value:new Ue},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:We.background_vert,fragmentShader:We.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Ue}},vertexShader:We.backgroundCube_vert,fragmentShader:We.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:We.cube_vert,fragmentShader:We.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:We.equirect_vert,fragmentShader:We.equirect_frag},distance:{uniforms:hn([fe.common,fe.displacementmap,{referencePosition:{value:new B},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:We.distance_vert,fragmentShader:We.distance_frag},shadow:{uniforms:hn([fe.lights,fe.fog,{color:{value:new it(0)},opacity:{value:1}}]),vertexShader:We.shadow_vert,fragmentShader:We.shadow_frag}};Fi.physical={uniforms:hn([Fi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Ue},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Ue},clearcoatNormalScale:{value:new ut(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Ue},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Ue},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Ue},sheen:{value:0},sheenColor:{value:new it(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Ue},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Ue},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Ue},transmissionSamplerSize:{value:new ut},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Ue},attenuationDistance:{value:0},attenuationColor:{value:new it(0)},specularColor:{value:new it(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Ue},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Ue},anisotropyVector:{value:new ut},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Ue}}]),vertexShader:We.meshphysical_vert,fragmentShader:We.meshphysical_frag};var Nd={r:0,b:0,g:0},a1=new Ot,Qx=new Ue;Qx.set(-1,0,0,0,1,0,0,0,1);function c1(n,e,t,i,r,s){let o=new it(0),a=r===!0?0:1,c,l,u=null,f=0,d=null;function h(b){let M=b.isScene===!0?b.background:null;if(M&&M.isTexture){let S=b.backgroundBlurriness>0;M=e.get(M,S)}return M}function g(b){let M=!1,S=h(b);S===null?m(o,a):S&&S.isColor&&(m(S,1),M=!0);let D=n.xr.getEnvironmentBlendMode();D==="additive"?t.buffers.color.setClear(0,0,0,1,s):D==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||M)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function x(b,M){let S=h(M);S&&(S.isCubeTexture||S.mapping===Ha)?(l===void 0&&(l=new Kn(new go(1,1,1),new kn({name:"BackgroundCubeMaterial",uniforms:_s(Fi.backgroundCube.uniforms),vertexShader:Fi.backgroundCube.vertexShader,fragmentShader:Fi.backgroundCube.fragmentShader,side:xn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(D,T,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=S,l.material.uniforms.backgroundBlurriness.value=M.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(a1.makeRotationFromEuler(M.backgroundRotation)).transpose(),S.isCubeTexture&&S.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(Qx),l.material.toneMapped=Je.getTransfer(S.colorSpace)!==at,(u!==S||f!==S.version||d!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,f=S.version,d=n.toneMapping),l.layers.enableAll(),b.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new Kn(new Fa(2,2),new kn({name:"BackgroundMaterial",uniforms:_s(Fi.background.uniforms),vertexShader:Fi.background.vertexShader,fragmentShader:Fi.background.fragmentShader,side:Qi,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,c.material.toneMapped=Je.getTransfer(S.colorSpace)!==at,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||f!==S.version||d!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,f=S.version,d=n.toneMapping),c.layers.enableAll(),b.unshift(c,c.geometry,c.material,0,0,null))}function m(b,M){b.getRGB(Nd,zm(n)),t.buffers.color.setClear(Nd.r,Nd.g,Nd.b,M,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(b,M=1){o.set(b),a=M,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(b){a=b,m(o,a)},render:g,addToRenderList:x,dispose:p}}function l1(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=d(null),s=r,o=!1;function a(C,L,$,X,P){let W=!1,k=f(C,X,$,L);s!==k&&(s=k,l(s.object)),W=h(C,X,$,P),W&&g(C,X,$,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(W||o)&&(o=!1,S(C,L,$,X),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function f(C,L,$,X){let P=X.wireframe===!0,W=i[L.id];W===void 0&&(W={},i[L.id]=W);let k=C.isInstancedMesh===!0?C.id:0,te=W[k];te===void 0&&(te={},W[k]=te);let ne=te[$.id];ne===void 0&&(ne={},te[$.id]=ne);let he=ne[P];return he===void 0&&(he=d(c()),ne[P]=he),he}function d(C){let L=[],$=[],X=[];for(let P=0;P<t;P++)L[P]=0,$[P]=0,X[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:L,enabledAttributes:$,attributeDivisors:X,object:C,attributes:{},index:null}}function h(C,L,$,X){let P=s.attributes,W=L.attributes,k=0,te=$.getAttributes();for(let ne in te)if(te[ne].location>=0){let be=P[ne],we=W[ne];if(we===void 0&&(ne==="instanceMatrix"&&C.instanceMatrix&&(we=C.instanceMatrix),ne==="instanceColor"&&C.instanceColor&&(we=C.instanceColor)),be===void 0||be.attribute!==we||we&&be.data!==we.data)return!0;k++}return s.attributesNum!==k||s.index!==X}function g(C,L,$,X){let P={},W=L.attributes,k=0,te=$.getAttributes();for(let ne in te)if(te[ne].location>=0){let be=W[ne];be===void 0&&(ne==="instanceMatrix"&&C.instanceMatrix&&(be=C.instanceMatrix),ne==="instanceColor"&&C.instanceColor&&(be=C.instanceColor));let we={};we.attribute=be,be&&be.data&&(we.data=be.data),P[ne]=we,k++}s.attributes=P,s.attributesNum=k,s.index=X}function x(){let C=s.newAttributes;for(let L=0,$=C.length;L<$;L++)C[L]=0}function m(C){p(C,0)}function p(C,L){let $=s.newAttributes,X=s.enabledAttributes,P=s.attributeDivisors;$[C]=1,X[C]===0&&(n.enableVertexAttribArray(C),X[C]=1),P[C]!==L&&(n.vertexAttribDivisor(C,L),P[C]=L)}function b(){let C=s.newAttributes,L=s.enabledAttributes;for(let $=0,X=L.length;$<X;$++)L[$]!==C[$]&&(n.disableVertexAttribArray($),L[$]=0)}function M(C,L,$,X,P,W,k){k===!0?n.vertexAttribIPointer(C,L,$,P,W):n.vertexAttribPointer(C,L,$,X,P,W)}function S(C,L,$,X){x();let P=X.attributes,W=$.getAttributes(),k=L.defaultAttributeValues;for(let te in W){let ne=W[te];if(ne.location>=0){let he=P[te];if(he===void 0&&(te==="instanceMatrix"&&C.instanceMatrix&&(he=C.instanceMatrix),te==="instanceColor"&&C.instanceColor&&(he=C.instanceColor)),he!==void 0){let be=he.normalized,we=he.itemSize,Qe=e.get(he);if(Qe===void 0)continue;let dt=Qe.buffer,ze=Qe.type,K=Qe.bytesPerElement,ge=ze===n.INT||ze===n.UNSIGNED_INT||he.gpuType===Xu;if(he.isInterleavedBufferAttribute){let ae=he.data,Re=ae.stride,Be=he.offset;if(ae.isInstancedInterleavedBuffer){for(let Pe=0;Pe<ne.locationSize;Pe++)p(ne.location+Pe,ae.meshPerAttribute);C.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=ae.meshPerAttribute*ae.count)}else for(let Pe=0;Pe<ne.locationSize;Pe++)m(ne.location+Pe);n.bindBuffer(n.ARRAY_BUFFER,dt);for(let Pe=0;Pe<ne.locationSize;Pe++)M(ne.location+Pe,we/ne.locationSize,ze,be,Re*K,(Be+we/ne.locationSize*Pe)*K,ge)}else{if(he.isInstancedBufferAttribute){for(let ae=0;ae<ne.locationSize;ae++)p(ne.location+ae,he.meshPerAttribute);C.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=he.meshPerAttribute*he.count)}else for(let ae=0;ae<ne.locationSize;ae++)m(ne.location+ae);n.bindBuffer(n.ARRAY_BUFFER,dt);for(let ae=0;ae<ne.locationSize;ae++)M(ne.location+ae,we/ne.locationSize,ze,be,we*K,we/ne.locationSize*ae*K,ge)}}else if(k!==void 0){let be=k[te];if(be!==void 0)switch(be.length){case 2:n.vertexAttrib2fv(ne.location,be);break;case 3:n.vertexAttrib3fv(ne.location,be);break;case 4:n.vertexAttrib4fv(ne.location,be);break;default:n.vertexAttrib1fv(ne.location,be)}}}}b()}function D(){w();for(let C in i){let L=i[C];for(let $ in L){let X=L[$];for(let P in X){let W=X[P];for(let k in W)u(W[k].object),delete W[k];delete X[P]}}delete i[C]}}function T(C){if(i[C.id]===void 0)return;let L=i[C.id];for(let $ in L){let X=L[$];for(let P in X){let W=X[P];for(let k in W)u(W[k].object),delete W[k];delete X[P]}}delete i[C.id]}function I(C){for(let L in i){let $=i[L];for(let X in $){let P=$[X];if(P[C.id]===void 0)continue;let W=P[C.id];for(let k in W)u(W[k].object),delete W[k];delete P[C.id]}}}function y(C){for(let L in i){let $=i[L],X=C.isInstancedMesh===!0?C.id:0,P=$[X];if(P!==void 0){for(let W in P){let k=P[W];for(let te in k)u(k[te].object),delete k[te];delete P[W]}delete $[X],Object.keys($).length===0&&delete i[L]}}}function w(){O(),o=!0,s!==r&&(s=r,l(s.object))}function O(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:w,resetDefaultState:O,dispose:D,releaseStatesOfGeometry:T,releaseStatesOfObject:y,releaseStatesOfProgram:I,initAttributes:x,enableAttribute:m,disableUnusedAttributes:b}}function u1(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let d=0;for(let h=0;h<u;h++)d+=l[h];t.update(d,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function d1(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(I){return!(I!==Qn&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let y=I===Li&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==Bn&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&I!==vi&&!y)}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Ae("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let f=t.logarithmicDepthBuffer===!0,d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&d===!1&&Ae("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),x=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),b=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),M=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),D=n.getParameter(n.MAX_SAMPLES),T=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:g,maxTextureSize:x,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:b,maxVaryings:M,maxFragmentUniforms:S,maxSamples:D,samples:T}}function f1(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Ci,a=new Ue,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(f,d){let h=f.length!==0||d||i!==0||r;return r=d,i=f.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(f,d){t=u(f,d,0)},this.setState=function(f,d,h){let g=f.clippingPlanes,x=f.clipIntersection,m=f.clipShadows,p=n.get(f);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let b=s?0:i,M=b*4,S=p.clippingState||null;c.value=S,S=u(g,d,M,h);for(let D=0;D!==M;++D)S[D]=t[D];p.clippingState=S,this.numIntersection=x?this.numPlanes:0,this.numPlanes+=b}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(f,d,h,g){let x=f!==null?f.length:0,m=null;if(x!==0){if(m=c.value,g!==!0||m===null){let p=h+x*4,b=d.matrixWorldInverse;a.getNormalMatrix(b),(m===null||m.length<p)&&(m=new Float32Array(p));for(let M=0,S=h;M!==x;++M,S+=4)o.copy(f[M]).applyMatrix4(b,a),o.normal.toArray(m,S),m[S+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=x,e.numIntersection=0,m}}var Or=4,Dx=[.125,.215,.35,.446,.526,.582],xs=20,h1=256,Za=new Ba,Rx=new it,$m=null,qm=0,Xm=0,Ym=!1,p1=new B,Ld=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=p1}=s;$m=this._renderer.getRenderTarget(),qm=this._renderer.getActiveCubeFace(),Xm=this._renderer.getActiveMipmapLevel(),Ym=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=Lx(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=Px(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget($m,qm,Xm),this._renderer.xr.enabled=Ym,e.scissorTest=!1,xo(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Rr||e.mapping===ys?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),$m=this._renderer.getRenderTarget(),qm=this._renderer.getActiveCubeFace(),Xm=this._renderer.getActiveMipmapLevel(),Ym=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:en,minFilter:en,generateMipmaps:!1,type:Li,format:Qn,colorSpace:xa,depthBuffer:!1},r=Nx(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=Nx(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=m1(s)),this._blurMaterial=v1(s,e,t),this._ggxMaterial=g1(s,e,t)}return r}_compileMaterial(e){let t=new Kn(new _n,e);this._renderer.compile(t,Za)}_sceneToCubeUV(e,t,i,r,s){let c=new dn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],f=this._renderer,d=f.autoClear,h=f.toneMapping;f.getClearColor(Rx),f.toneMapping=mi,f.autoClear=!1,f.state.buffers.depth.getReversed()&&(f.setRenderTarget(r),f.clearDepth(),f.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Kn(new go,new Da({name:"PMREM.Background",side:xn,depthWrite:!1,depthTest:!1})));let x=this._backgroundBox,m=x.material,p=!1,b=e.background;b?b.isColor&&(m.color.copy(b),e.background=null,p=!0):(m.color.copy(Rx),p=!0);for(let M=0;M<6;M++){let S=M%3;S===0?(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[M],s.y,s.z)):S===1?(c.up.set(0,0,l[M]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[M],s.z)):(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[M]));let D=this._cubeSize;xo(r,S*D,M>2?D:0,D,D),f.setRenderTarget(r),p&&f.render(x,c),f.render(e,c)}f.toneMapping=h,f.autoClear=d,e.background=b}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Rr||e.mapping===ys;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=Lx()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=Px());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;xo(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,Za)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),f=Math.sqrt(l*l-u*u),d=0+l*1.25,h=f*d,{_lodMax:g}=this,x=this._sizeLods[i],m=3*x*(i>g-Or?i-g+Or:0),p=4*(this._cubeSize-x);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,xo(s,m,p,3*x,2*x),r.setRenderTarget(s),r.render(a,Za),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,xo(e,m,p,3*x,2*x),r.setRenderTarget(e),r.render(a,Za)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Ne("blur direction must be either latitudinal or longitudinal!");let u=3,f=this._lodMeshes[r];f.material=l;let d=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*xs-1),x=s/g,m=isFinite(s)?1+Math.floor(u*x):xs;m>xs&&Ae(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${xs}`);let p=[],b=0;for(let I=0;I<xs;++I){let y=I/x,w=Math.exp(-y*y/2);p.push(w),I===0?b+=w:I<m&&(b+=2*w)}for(let I=0;I<p.length;I++)p[I]=p[I]/b;d.envMap.value=e.texture,d.samples.value=m,d.weights.value=p,d.latitudinal.value=o==="latitudinal",a&&(d.poleAxis.value=a);let{_lodMax:M}=this;d.dTheta.value=g,d.mipInt.value=M-i;let S=this._sizeLods[r],D=3*S*(r>M-Or?r-M+Or:0),T=4*(this._cubeSize-S);xo(t,D,T,3*S,2*S),c.setRenderTarget(t),c.render(f,Za)}};function m1(n){let e=[],t=[],i=[],r=n,s=n-Or+1+Dx.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-Or?c=Dx[o-n+Or-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,f=1+l,d=[u,u,f,u,f,f,u,u,f,f,u,f],h=6,g=6,x=3,m=2,p=1,b=new Float32Array(x*g*h),M=new Float32Array(m*g*h),S=new Float32Array(p*g*h);for(let T=0;T<h;T++){let I=T%3*2/3-1,y=T>2?0:-1,w=[I,y,0,I+2/3,y,0,I+2/3,y+1,0,I,y,0,I+2/3,y+1,0,I,y+1,0];b.set(w,x*g*T),M.set(d,m*g*T);let O=[T,T,T,T,T,T];S.set(O,p*g*T)}let D=new _n;D.setAttribute("position",new fn(b,x)),D.setAttribute("uv",new fn(M,m)),D.setAttribute("faceIndex",new fn(S,p)),i.push(new Kn(D,null)),r>Or&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function Nx(n,e,t){let i=new Fn(n,e,t);return i.texture.mapping=Ha,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function xo(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function g1(n,e,t){return new kn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:h1,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:kd(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:Pi,depthTest:!1,depthWrite:!1})}function v1(n,e,t){let i=new Float32Array(xs),r=new B(0,1,0);return new kn({name:"SphericalGaussianBlur",defines:{n:xs,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:kd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Pi,depthTest:!1,depthWrite:!1})}function Px(){return new kn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:kd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Pi,depthTest:!1,depthWrite:!1})}function Lx(){return new kn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:kd(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Pi,depthTest:!1,depthWrite:!1})}function kd(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var Od=class extends Fn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new La(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new go(5,5,5),s=new kn({name:"CubemapFromEquirect",uniforms:_s(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:xn,blending:Pi});s.uniforms.tEquirect.value=t;let o=new Kn(r,s),a=t.minFilter;return t.minFilter===Nr&&(t.minFilter=en),new Gu(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function y1(n){let e=new WeakMap,t=new WeakMap,i=null;function r(d,h=!1){return d==null?null:h?o(d):s(d)}function s(d){if(d&&d.isTexture){let h=d.mapping;if(h===ju||h===$u)if(e.has(d)){let g=e.get(d).texture;return a(g,d.mapping)}else{let g=d.image;if(g&&g.height>0){let x=new Od(g.height);return x.fromEquirectangularTexture(n,d),e.set(d,x),d.addEventListener("dispose",l),a(x.texture,d.mapping)}else return null}}return d}function o(d){if(d&&d.isTexture){let h=d.mapping,g=h===ju||h===$u,x=h===Rr||h===ys;if(g||x){let m=t.get(d),p=m!==void 0?m.texture.pmremVersion:0;if(d.isRenderTargetTexture&&d.pmremVersion!==p)return i===null&&(i=new Ld(n)),m=g?i.fromEquirectangular(d,m):i.fromCubemap(d,m),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),m.texture;if(m!==void 0)return m.texture;{let b=d.image;return g&&b&&b.height>0||x&&b&&c(b)?(i===null&&(i=new Ld(n)),m=g?i.fromEquirectangular(d):i.fromCubemap(d),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),d.addEventListener("dispose",u),m.texture):null}}}return d}function a(d,h){return h===ju?d.mapping=Rr:h===$u&&(d.mapping=ys),d}function c(d){let h=0,g=6;for(let x=0;x<g;x++)d[x]!==void 0&&h++;return h===g}function l(d){let h=d.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(d){let h=d.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function f(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:f}}function _1(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&bu("WebGLRenderer: "+i+" extension not supported."),r}}}function x1(n,e,t,i){let r={},s=new WeakMap;function o(f){let d=f.target;d.index!==null&&e.remove(d.index);for(let g in d.attributes)e.remove(d.attributes[g]);d.removeEventListener("dispose",o),delete r[d.id];let h=s.get(d);h&&(e.remove(h),s.delete(d)),i.releaseStatesOfGeometry(d),d.isInstancedBufferGeometry===!0&&delete d._maxInstanceCount,t.memory.geometries--}function a(f,d){return r[d.id]===!0||(d.addEventListener("dispose",o),r[d.id]=!0,t.memory.geometries++),d}function c(f){let d=f.attributes;for(let h in d)e.update(d[h],n.ARRAY_BUFFER)}function l(f){let d=[],h=f.index,g=f.attributes.position,x=0;if(g===void 0)return;if(h!==null){let b=h.array;x=h.version;for(let M=0,S=b.length;M<S;M+=3){let D=b[M+0],T=b[M+1],I=b[M+2];d.push(D,T,T,I,I,D)}}else{let b=g.array;x=g.version;for(let M=0,S=b.length/3-1;M<S;M+=3){let D=M+0,T=M+1,I=M+2;d.push(D,T,T,I,I,D)}}let m=new(g.count>=65535?Aa:Ia)(d,1);m.version=x;let p=s.get(f);p&&e.remove(p),s.set(f,m)}function u(f){let d=s.get(f);if(d){let h=f.index;h!==null&&d.version<h.version&&l(f)}else l(f);return s.get(f)}return{get:a,update:c,getWireframeAttribute:u}}function E1(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function c(f,d){n.drawElements(i,d,s,f*o),t.update(d,i,1)}function l(f,d,h){h!==0&&(n.drawElementsInstanced(i,d,s,f*o,h),t.update(d,i,h))}function u(f,d,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,d,0,s,f,0,h);let x=0;for(let m=0;m<h;m++)x+=d[m];t.update(x,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function b1(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Ne("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function M1(n,e,t){let i=new WeakMap,r=new Ft;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,f=u!==void 0?u.length:0,d=i.get(a);if(d===void 0||d.count!==f){let O=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",O)};var h=O;d!==void 0&&d.texture.dispose();let g=a.morphAttributes.position!==void 0,x=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],b=a.morphAttributes.normal||[],M=a.morphAttributes.color||[],S=0;g===!0&&(S=1),x===!0&&(S=2),m===!0&&(S=3);let D=a.attributes.position.count*S,T=1;D>e.maxTextureSize&&(T=Math.ceil(D/e.maxTextureSize),D=e.maxTextureSize);let I=new Float32Array(D*T*4*f),y=new Sa(I,D,T,f);y.type=vi,y.needsUpdate=!0;let w=S*4;for(let C=0;C<f;C++){let L=p[C],$=b[C],X=M[C],P=D*T*4*C;for(let W=0;W<L.count;W++){let k=W*w;g===!0&&(r.fromBufferAttribute(L,W),I[P+k+0]=r.x,I[P+k+1]=r.y,I[P+k+2]=r.z,I[P+k+3]=0),x===!0&&(r.fromBufferAttribute($,W),I[P+k+4]=r.x,I[P+k+5]=r.y,I[P+k+6]=r.z,I[P+k+7]=0),m===!0&&(r.fromBufferAttribute(X,W),I[P+k+8]=r.x,I[P+k+9]=r.y,I[P+k+10]=r.z,I[P+k+11]=X.itemSize===4?r.w:1)}}d={count:f,texture:y,size:new ut(D,T)},i.set(a,d),a.addEventListener("dispose",O)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let x=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",x),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",d.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",d.size)}return{update:s}}function S1(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,f=l.geometry,d=e.get(l,f);if(s.get(d)!==u&&(e.update(d),s.set(d,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return d}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var T1={[Tm]:"LINEAR_TONE_MAPPING",[wm]:"REINHARD_TONE_MAPPING",[Cm]:"CINEON_TONE_MAPPING",[Im]:"ACES_FILMIC_TONE_MAPPING",[Dm]:"AGX_TONE_MAPPING",[Rm]:"NEUTRAL_TONE_MAPPING",[Am]:"CUSTOM_TONE_MAPPING"};function w1(n,e,t,i,r){let s=new Fn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new tr(e,t):void 0}),o=new Fn(e,t,{type:Li,depthBuffer:!1,stencilBuffer:!1}),a=new _n;a.setAttribute("position",new Tn([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Tn([0,2,0,0,2,0],2));let c=new Nu({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Kn(a,c),u=new Ba(-1,1,1,-1,0,1),f=null,d=null,h=!1,g,x=null,m=[],p=!1;this.setSize=function(b,M){s.setSize(b,M),o.setSize(b,M);for(let S=0;S<m.length;S++){let D=m[S];D.setSize&&D.setSize(b,M)}},this.setEffects=function(b){m=b,p=m.length>0&&m[0].isRenderPass===!0;let M=s.width,S=s.height;for(let D=0;D<m.length;D++){let T=m[D];T.setSize&&T.setSize(M,S)}},this.begin=function(b,M){if(h||b.toneMapping===mi&&m.length===0)return!1;if(x=M,M!==null){let S=M.width,D=M.height;(s.width!==S||s.height!==D)&&this.setSize(S,D)}return p===!1&&b.setRenderTarget(s),g=b.toneMapping,b.toneMapping=mi,!0},this.hasRenderPass=function(){return p},this.end=function(b,M){b.toneMapping=g,h=!0;let S=s,D=o;for(let T=0;T<m.length;T++){let I=m[T];if(I.enabled!==!1&&(I.render(b,D,S,M),I.needsSwap!==!1)){let y=S;S=D,D=y}}if(f!==b.outputColorSpace||d!==b.toneMapping){f=b.outputColorSpace,d=b.toneMapping,c.defines={},Je.getTransfer(f)===at&&(c.defines.SRGB_TRANSFER="");let T=T1[d];T&&(c.defines[T]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=S.texture,b.setRenderTarget(x),b.render(l,u),x=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var eE=new ir,Km=new tr(1,1),tE=new Sa,nE=new wu,iE=new La,Ox=[],Fx=[],kx=new Float32Array(16),Ux=new Float32Array(9),Bx=new Float32Array(4);function bo(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=Ox[r];if(s===void 0&&(s=new Float32Array(r),Ox[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function qt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Xt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Ud(n,e){let t=Fx[e];t===void 0&&(t=new Int32Array(e),Fx[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function C1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function I1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2fv(this.addr,e),Xt(t,e)}}function A1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(qt(t,e))return;n.uniform3fv(this.addr,e),Xt(t,e)}}function D1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4fv(this.addr,e),Xt(t,e)}}function R1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;Bx.set(i),n.uniformMatrix2fv(this.addr,!1,Bx),Xt(t,i)}}function N1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;Ux.set(i),n.uniformMatrix3fv(this.addr,!1,Ux),Xt(t,i)}}function P1(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(qt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Xt(t,e)}else{if(qt(t,i))return;kx.set(i),n.uniformMatrix4fv(this.addr,!1,kx),Xt(t,i)}}function L1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function O1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2iv(this.addr,e),Xt(t,e)}}function F1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3iv(this.addr,e),Xt(t,e)}}function k1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4iv(this.addr,e),Xt(t,e)}}function U1(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function B1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(qt(t,e))return;n.uniform2uiv(this.addr,e),Xt(t,e)}}function V1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(qt(t,e))return;n.uniform3uiv(this.addr,e),Xt(t,e)}}function H1(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(qt(t,e))return;n.uniform4uiv(this.addr,e),Xt(t,e)}}function z1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Km.compareFunction=t.isReversedDepthBuffer()?Rd:Dd,s=Km):s=eE,t.setTexture2D(e||s,r)}function G1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||nE,r)}function W1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||iE,r)}function j1(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||tE,r)}function $1(n){switch(n){case 5126:return C1;case 35664:return I1;case 35665:return A1;case 35666:return D1;case 35674:return R1;case 35675:return N1;case 35676:return P1;case 5124:case 35670:return L1;case 35667:case 35671:return O1;case 35668:case 35672:return F1;case 35669:case 35673:return k1;case 5125:return U1;case 36294:return B1;case 36295:return V1;case 36296:return H1;case 35678:case 36198:case 36298:case 36306:case 35682:return z1;case 35679:case 36299:case 36307:return G1;case 35680:case 36300:case 36308:case 36293:return W1;case 36289:case 36303:case 36311:case 36292:return j1}}function q1(n,e){n.uniform1fv(this.addr,e)}function X1(n,e){let t=bo(e,this.size,2);n.uniform2fv(this.addr,t)}function Y1(n,e){let t=bo(e,this.size,3);n.uniform3fv(this.addr,t)}function Z1(n,e){let t=bo(e,this.size,4);n.uniform4fv(this.addr,t)}function J1(n,e){let t=bo(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function K1(n,e){let t=bo(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function Q1(n,e){let t=bo(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function eD(n,e){n.uniform1iv(this.addr,e)}function tD(n,e){n.uniform2iv(this.addr,e)}function nD(n,e){n.uniform3iv(this.addr,e)}function iD(n,e){n.uniform4iv(this.addr,e)}function rD(n,e){n.uniform1uiv(this.addr,e)}function sD(n,e){n.uniform2uiv(this.addr,e)}function oD(n,e){n.uniform3uiv(this.addr,e)}function aD(n,e){n.uniform4uiv(this.addr,e)}function cD(n,e,t){let i=this.cache,r=e.length,s=Ud(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Km:o=eE;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function lD(n,e,t){let i=this.cache,r=e.length,s=Ud(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||nE,s[o])}function uD(n,e,t){let i=this.cache,r=e.length,s=Ud(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||iE,s[o])}function dD(n,e,t){let i=this.cache,r=e.length,s=Ud(t,r);qt(i,s)||(n.uniform1iv(this.addr,s),Xt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||tE,s[o])}function fD(n){switch(n){case 5126:return q1;case 35664:return X1;case 35665:return Y1;case 35666:return Z1;case 35674:return J1;case 35675:return K1;case 35676:return Q1;case 5124:case 35670:return eD;case 35667:case 35671:return tD;case 35668:case 35672:return nD;case 35669:case 35673:return iD;case 5125:return rD;case 36294:return sD;case 36295:return oD;case 36296:return aD;case 35678:case 36198:case 36298:case 36306:case 35682:return cD;case 35679:case 36299:case 36307:return lD;case 35680:case 36300:case 36308:case 36293:return uD;case 36289:case 36303:case 36311:case 36292:return dD}}var Qm=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=$1(t.type)}},eg=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=fD(t.type)}},tg=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Zm=/(\w+)(\])?(\[|\.)?/g;function Vx(n,e){n.seq.push(e),n.map[e.id]=e}function hD(n,e,t){let i=n.name,r=i.length;for(Zm.lastIndex=0;;){let s=Zm.exec(i),o=Zm.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){Vx(t,l===void 0?new Qm(a,n,e):new eg(a,n,e));break}else{let f=t.map[a];f===void 0&&(f=new tg(a),Vx(t,f)),t=f}}}var Eo=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);hD(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function Hx(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var pD=37297,mD=0;function gD(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var zx=new Ue;function vD(n){Je._getMatrix(zx,Je.workingColorSpace,n);let e=`mat3( ${zx.elements.map(t=>t.toFixed(4))} )`;switch(Je.getTransfer(n)){case Ea:return[e,"LinearTransferOETF"];case at:return[e,"sRGBTransferOETF"];default:return Ae("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function Gx(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+gD(n.getShaderSource(e),a)}else return s}function yD(n,e){let t=vD(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var _D={[Tm]:"Linear",[wm]:"Reinhard",[Cm]:"Cineon",[Im]:"ACESFilmic",[Dm]:"AgX",[Rm]:"Neutral",[Am]:"Custom"};function xD(n,e){let t=_D[e];return t===void 0?(Ae("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Pd=new B;function ED(){Je.getLuminanceCoefficients(Pd);let n=Pd.x.toFixed(4),e=Pd.y.toFixed(4),t=Pd.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function bD(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Ka).join(`
`)}function MD(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function SD(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Ka(n){return n!==""}function Wx(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function jx(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var TD=/^[ \t]*#include +<([\w\d./]+)>/gm;function ng(n){return n.replace(TD,CD)}var wD=new Map;function CD(n,e){let t=We[e];if(t===void 0){let i=wD.get(e);if(i!==void 0)t=We[i],Ae('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return ng(t)}var ID=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function $x(n){return n.replace(ID,AD)}function AD(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function qx(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var DD={[Va]:"SHADOWMAP_TYPE_PCF",[vo]:"SHADOWMAP_TYPE_VSM"};function RD(n){return DD[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var ND={[Rr]:"ENVMAP_TYPE_CUBE",[ys]:"ENVMAP_TYPE_CUBE",[Ha]:"ENVMAP_TYPE_CUBE_UV"};function PD(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":ND[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var LD={[ys]:"ENVMAP_MODE_REFRACTION"};function OD(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":LD[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var FD={[Sm]:"ENVMAP_BLENDING_MULTIPLY",[hx]:"ENVMAP_BLENDING_MIX",[px]:"ENVMAP_BLENDING_ADD"};function kD(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":FD[n.combine]||"ENVMAP_BLENDING_NONE"}function UD(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function BD(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=RD(t),l=PD(t),u=OD(t),f=kD(t),d=UD(t),h=bD(t),g=MD(s),x=r.createProgram(),m,p,b=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ka).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Ka).join(`
`),p.length>0&&(p+=`
`)):(m=[qx(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Ka).join(`
`),p=[qx(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+f:"",d?"#define CUBEUV_TEXEL_WIDTH "+d.texelWidth:"",d?"#define CUBEUV_TEXEL_HEIGHT "+d.texelHeight:"",d?"#define CUBEUV_MAX_MIP "+d.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==mi?"#define TONE_MAPPING":"",t.toneMapping!==mi?We.tonemapping_pars_fragment:"",t.toneMapping!==mi?xD("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",We.colorspace_pars_fragment,yD("linearToOutputTexel",t.outputColorSpace),ED(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Ka).join(`
`)),o=ng(o),o=Wx(o,t),o=jx(o,t),a=ng(a),a=Wx(a,t),a=jx(a,t),o=$x(o),a=$x(a),t.isRawShaderMaterial!==!0&&(b=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===Vm?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Vm?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let M=b+m+o,S=b+p+a,D=Hx(r,r.VERTEX_SHADER,M),T=Hx(r,r.FRAGMENT_SHADER,S);r.attachShader(x,D),r.attachShader(x,T),t.index0AttributeName!==void 0?r.bindAttribLocation(x,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(x,0,"position"),r.linkProgram(x);function I(C){if(n.debug.checkShaderErrors){let L=r.getProgramInfoLog(x)||"",$=r.getShaderInfoLog(D)||"",X=r.getShaderInfoLog(T)||"",P=L.trim(),W=$.trim(),k=X.trim(),te=!0,ne=!0;if(r.getProgramParameter(x,r.LINK_STATUS)===!1)if(te=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,x,D,T);else{let he=Gx(r,D,"vertex"),be=Gx(r,T,"fragment");Ne("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(x,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+P+`
`+he+`
`+be)}else P!==""?Ae("WebGLProgram: Program Info Log:",P):(W===""||k==="")&&(ne=!1);ne&&(C.diagnostics={runnable:te,programLog:P,vertexShader:{log:W,prefix:m},fragmentShader:{log:k,prefix:p}})}r.deleteShader(D),r.deleteShader(T),y=new Eo(r,x),w=SD(r,x)}let y;this.getUniforms=function(){return y===void 0&&I(this),y};let w;this.getAttributes=function(){return w===void 0&&I(this),w};let O=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return O===!1&&(O=r.getProgramParameter(x,pD)),O},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(x),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=mD++,this.cacheKey=e,this.usedTimes=1,this.program=x,this.vertexShader=D,this.fragmentShader=T,this}var VD=0,ig=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new rg(e),t.set(e,i)),i}},rg=class{constructor(e){this.id=VD++,this.code=e,this.usedTimes=0}};function HD(n){return n===Lr||n===qa||n===Xa}function zD(n,e,t,i,r,s){let o=new wa,a=new ig,c=new Set,l=[],u=new Map,f=i.logarithmicDepthBuffer,d=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(y){return c.add(y),y===0?"uv":`uv${y}`}function x(y,w,O,C,L,$){let X=C.fog,P=L.geometry,W=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?C.environment:null,k=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,te=e.get(y.envMap||W,k),ne=te&&te.mapping===Ha?te.image.height:null,he=h[y.type];y.precision!==null&&(d=i.getMaxPrecision(y.precision),d!==y.precision&&Ae("WebGLProgram.getParameters:",y.precision,"not supported, using",d,"instead."));let be=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,we=be!==void 0?be.length:0,Qe=0;P.morphAttributes.position!==void 0&&(Qe=1),P.morphAttributes.normal!==void 0&&(Qe=2),P.morphAttributes.color!==void 0&&(Qe=3);let dt,ze,K,ge;if(he){let Ve=Fi[he];dt=Ve.vertexShader,ze=Ve.fragmentShader}else dt=y.vertexShader,ze=y.fragmentShader,a.update(y),K=a.getVertexShaderID(y),ge=a.getFragmentShaderID(y);let ae=n.getRenderTarget(),Re=n.state.buffers.depth.getReversed(),Be=L.isInstancedMesh===!0,Pe=L.isBatchedMesh===!0,wt=!!y.map,Ye=!!y.matcap,ft=!!te,Mt=!!y.aoMap,qe=!!y.lightMap,Wt=!!y.bumpMap,Ct=!!y.normalMap,wn=!!y.displacementMap,R=!!y.emissiveMap,jt=!!y.metalnessMap,Ze=!!y.roughnessMap,Et=y.anisotropy>0,de=y.clearcoat>0,Dt=y.dispersion>0,E=y.iridescence>0,v=y.sheen>0,F=y.transmission>0,Z=Et&&!!y.anisotropyMap,ee=de&&!!y.clearcoatMap,ie=de&&!!y.clearcoatNormalMap,ue=de&&!!y.clearcoatRoughnessMap,q=E&&!!y.iridescenceMap,J=E&&!!y.iridescenceThicknessMap,ve=v&&!!y.sheenColorMap,xe=v&&!!y.sheenRoughnessMap,ce=!!y.specularMap,re=!!y.specularColorMap,Oe=!!y.specularIntensityMap,Ge=F&&!!y.transmissionMap,rt=F&&!!y.thicknessMap,A=!!y.gradientMap,se=!!y.alphaMap,Y=y.alphaTest>0,ye=!!y.alphaHash,le=!!y.extensions,Q=mi;y.toneMapped&&(ae===null||ae.isXRRenderTarget===!0)&&(Q=n.toneMapping);let Se={shaderID:he,shaderType:y.type,shaderName:y.name,vertexShader:dt,fragmentShader:ze,defines:y.defines,customVertexShaderID:K,customFragmentShaderID:ge,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:d,batching:Pe,batchingColor:Pe&&L._colorsTexture!==null,instancing:Be,instancingColor:Be&&L.instanceColor!==null,instancingMorph:Be&&L.morphTexture!==null,outputColorSpace:ae===null?n.outputColorSpace:ae.isXRRenderTarget===!0?ae.texture.colorSpace:Je.workingColorSpace,alphaToCoverage:!!y.alphaToCoverage,map:wt,matcap:Ye,envMap:ft,envMapMode:ft&&te.mapping,envMapCubeUVHeight:ne,aoMap:Mt,lightMap:qe,bumpMap:Wt,normalMap:Ct,displacementMap:wn,emissiveMap:R,normalMapObjectSpace:Ct&&y.normalMapType===vx,normalMapTangentSpace:Ct&&y.normalMapType===Bm,packedNormalMap:Ct&&y.normalMapType===Bm&&HD(y.normalMap.format),metalnessMap:jt,roughnessMap:Ze,anisotropy:Et,anisotropyMap:Z,clearcoat:de,clearcoatMap:ee,clearcoatNormalMap:ie,clearcoatRoughnessMap:ue,dispersion:Dt,iridescence:E,iridescenceMap:q,iridescenceThicknessMap:J,sheen:v,sheenColorMap:ve,sheenRoughnessMap:xe,specularMap:ce,specularColorMap:re,specularIntensityMap:Oe,transmission:F,transmissionMap:Ge,thicknessMap:rt,gradientMap:A,opaque:y.transparent===!1&&y.blending===gs&&y.alphaToCoverage===!1,alphaMap:se,alphaTest:Y,alphaHash:ye,combine:y.combine,mapUv:wt&&g(y.map.channel),aoMapUv:Mt&&g(y.aoMap.channel),lightMapUv:qe&&g(y.lightMap.channel),bumpMapUv:Wt&&g(y.bumpMap.channel),normalMapUv:Ct&&g(y.normalMap.channel),displacementMapUv:wn&&g(y.displacementMap.channel),emissiveMapUv:R&&g(y.emissiveMap.channel),metalnessMapUv:jt&&g(y.metalnessMap.channel),roughnessMapUv:Ze&&g(y.roughnessMap.channel),anisotropyMapUv:Z&&g(y.anisotropyMap.channel),clearcoatMapUv:ee&&g(y.clearcoatMap.channel),clearcoatNormalMapUv:ie&&g(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ue&&g(y.clearcoatRoughnessMap.channel),iridescenceMapUv:q&&g(y.iridescenceMap.channel),iridescenceThicknessMapUv:J&&g(y.iridescenceThicknessMap.channel),sheenColorMapUv:ve&&g(y.sheenColorMap.channel),sheenRoughnessMapUv:xe&&g(y.sheenRoughnessMap.channel),specularMapUv:ce&&g(y.specularMap.channel),specularColorMapUv:re&&g(y.specularColorMap.channel),specularIntensityMapUv:Oe&&g(y.specularIntensityMap.channel),transmissionMapUv:Ge&&g(y.transmissionMap.channel),thicknessMapUv:rt&&g(y.thicknessMap.channel),alphaMapUv:se&&g(y.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(Ct||Et),vertexNormals:!!P.attributes.normal,vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:L.isPoints===!0&&!!P.attributes.uv&&(wt||se),fog:!!X,useFog:y.fog===!0,fogExp2:!!X&&X.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||P.attributes.normal===void 0&&Ct===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:f,reversedDepthBuffer:Re,skinning:L.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:we,morphTextureStride:Qe,numDirLights:w.directional.length,numPointLights:w.point.length,numSpotLights:w.spot.length,numSpotLightMaps:w.spotLightMap.length,numRectAreaLights:w.rectArea.length,numHemiLights:w.hemi.length,numDirLightShadows:w.directionalShadowMap.length,numPointLightShadows:w.pointShadowMap.length,numSpotLightShadows:w.spotShadowMap.length,numSpotLightShadowsWithMaps:w.numSpotLightShadowsWithMaps,numLightProbes:w.numLightProbes,numLightProbeGrids:$.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&O.length>0,shadowMapType:n.shadowMap.type,toneMapping:Q,decodeVideoTexture:wt&&y.map.isVideoTexture===!0&&Je.getTransfer(y.map.colorSpace)===at,decodeVideoTextureEmissive:R&&y.emissiveMap.isVideoTexture===!0&&Je.getTransfer(y.emissiveMap.colorSpace)===at,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===Ni,flipSided:y.side===xn,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:le&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(le&&y.extensions.multiDraw===!0||Pe)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return Se.vertexUv1s=c.has(1),Se.vertexUv2s=c.has(2),Se.vertexUv3s=c.has(3),c.clear(),Se}function m(y){let w=[];if(y.shaderID?w.push(y.shaderID):(w.push(y.customVertexShaderID),w.push(y.customFragmentShaderID)),y.defines!==void 0)for(let O in y.defines)w.push(O),w.push(y.defines[O]);return y.isRawShaderMaterial===!1&&(p(w,y),b(w,y),w.push(n.outputColorSpace)),w.push(y.customProgramCacheKey),w.join()}function p(y,w){y.push(w.precision),y.push(w.outputColorSpace),y.push(w.envMapMode),y.push(w.envMapCubeUVHeight),y.push(w.mapUv),y.push(w.alphaMapUv),y.push(w.lightMapUv),y.push(w.aoMapUv),y.push(w.bumpMapUv),y.push(w.normalMapUv),y.push(w.displacementMapUv),y.push(w.emissiveMapUv),y.push(w.metalnessMapUv),y.push(w.roughnessMapUv),y.push(w.anisotropyMapUv),y.push(w.clearcoatMapUv),y.push(w.clearcoatNormalMapUv),y.push(w.clearcoatRoughnessMapUv),y.push(w.iridescenceMapUv),y.push(w.iridescenceThicknessMapUv),y.push(w.sheenColorMapUv),y.push(w.sheenRoughnessMapUv),y.push(w.specularMapUv),y.push(w.specularColorMapUv),y.push(w.specularIntensityMapUv),y.push(w.transmissionMapUv),y.push(w.thicknessMapUv),y.push(w.combine),y.push(w.fogExp2),y.push(w.sizeAttenuation),y.push(w.morphTargetsCount),y.push(w.morphAttributeCount),y.push(w.numDirLights),y.push(w.numPointLights),y.push(w.numSpotLights),y.push(w.numSpotLightMaps),y.push(w.numHemiLights),y.push(w.numRectAreaLights),y.push(w.numDirLightShadows),y.push(w.numPointLightShadows),y.push(w.numSpotLightShadows),y.push(w.numSpotLightShadowsWithMaps),y.push(w.numLightProbes),y.push(w.shadowMapType),y.push(w.toneMapping),y.push(w.numClippingPlanes),y.push(w.numClipIntersection),y.push(w.depthPacking)}function b(y,w){o.disableAll(),w.instancing&&o.enable(0),w.instancingColor&&o.enable(1),w.instancingMorph&&o.enable(2),w.matcap&&o.enable(3),w.envMap&&o.enable(4),w.normalMapObjectSpace&&o.enable(5),w.normalMapTangentSpace&&o.enable(6),w.clearcoat&&o.enable(7),w.iridescence&&o.enable(8),w.alphaTest&&o.enable(9),w.vertexColors&&o.enable(10),w.vertexAlphas&&o.enable(11),w.vertexUv1s&&o.enable(12),w.vertexUv2s&&o.enable(13),w.vertexUv3s&&o.enable(14),w.vertexTangents&&o.enable(15),w.anisotropy&&o.enable(16),w.alphaHash&&o.enable(17),w.batching&&o.enable(18),w.dispersion&&o.enable(19),w.batchingColor&&o.enable(20),w.gradientMap&&o.enable(21),w.packedNormalMap&&o.enable(22),w.vertexNormals&&o.enable(23),y.push(o.mask),o.disableAll(),w.fog&&o.enable(0),w.useFog&&o.enable(1),w.flatShading&&o.enable(2),w.logarithmicDepthBuffer&&o.enable(3),w.reversedDepthBuffer&&o.enable(4),w.skinning&&o.enable(5),w.morphTargets&&o.enable(6),w.morphNormals&&o.enable(7),w.morphColors&&o.enable(8),w.premultipliedAlpha&&o.enable(9),w.shadowMapEnabled&&o.enable(10),w.doubleSided&&o.enable(11),w.flipSided&&o.enable(12),w.useDepthPacking&&o.enable(13),w.dithering&&o.enable(14),w.transmission&&o.enable(15),w.sheen&&o.enable(16),w.opaque&&o.enable(17),w.pointsUvs&&o.enable(18),w.decodeVideoTexture&&o.enable(19),w.decodeVideoTextureEmissive&&o.enable(20),w.alphaToCoverage&&o.enable(21),w.numLightProbeGrids>0&&o.enable(22),y.push(o.mask)}function M(y){let w=h[y.type],O;if(w){let C=Fi[w];O=Ax.clone(C.uniforms)}else O=y.uniforms;return O}function S(y,w){let O=u.get(w);return O!==void 0?++O.usedTimes:(O=new BD(n,w,y,r),l.push(O),u.set(w,O)),O}function D(y){if(--y.usedTimes===0){let w=l.indexOf(y);l[w]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function T(y){a.remove(y)}function I(){a.dispose()}return{getParameters:x,getProgramCacheKey:m,getUniforms:M,acquireProgram:S,releaseProgram:D,releaseShaderCache:T,programs:l,dispose:I}}function GD(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function WD(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function Xx(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function Yx(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d){let h=0;return d.isInstancedMesh&&(h+=2),d.isSkinnedMesh&&(h+=1),h}function a(d,h,g,x,m,p){let b=n[e];return b===void 0?(b={id:d.id,object:d,geometry:h,material:g,materialVariant:o(d),groupOrder:x,renderOrder:d.renderOrder,z:m,group:p},n[e]=b):(b.id=d.id,b.object=d,b.geometry=h,b.material=g,b.materialVariant=o(d),b.groupOrder=x,b.renderOrder=d.renderOrder,b.z=m,b.group=p),e++,b}function c(d,h,g,x,m,p){let b=a(d,h,g,x,m,p);g.transmission>0?i.push(b):g.transparent===!0?r.push(b):t.push(b)}function l(d,h,g,x,m,p){let b=a(d,h,g,x,m,p);g.transmission>0?i.unshift(b):g.transparent===!0?r.unshift(b):t.unshift(b)}function u(d,h){t.length>1&&t.sort(d||WD),i.length>1&&i.sort(h||Xx),r.length>1&&r.sort(h||Xx)}function f(){for(let d=e,h=n.length;d<h;d++){let g=n[d];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:f,sort:u}}function jD(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new Yx,n.set(i,[o])):r>=s.length?(o=new Yx,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function $D(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new B,color:new it};break;case"SpotLight":t={position:new B,direction:new B,color:new it,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new B,color:new it,distance:0,decay:0};break;case"HemisphereLight":t={direction:new B,skyColor:new it,groundColor:new it};break;case"RectAreaLight":t={color:new it,position:new B,halfWidth:new B,halfHeight:new B};break}return n[e.id]=t,t}}}function qD(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ut};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ut};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ut,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var XD=0;function YD(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function ZD(n){let e=new $D,t=qD(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new B);let r=new B,s=new Ot,o=new Ot;function a(l){let u=0,f=0,d=0;for(let w=0;w<9;w++)i.probe[w].set(0,0,0);let h=0,g=0,x=0,m=0,p=0,b=0,M=0,S=0,D=0,T=0,I=0;l.sort(YD);for(let w=0,O=l.length;w<O;w++){let C=l[w],L=C.color,$=C.intensity,X=C.distance,P=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===Lr?P=C.shadow.map.texture:P=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=L.r*$,f+=L.g*$,d+=L.b*$;else if(C.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(C.sh.coefficients[W],$);I++}else if(C.isDirectionalLight){let W=e.get(C);if(W.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let k=C.shadow,te=t.get(C);te.shadowIntensity=k.intensity,te.shadowBias=k.bias,te.shadowNormalBias=k.normalBias,te.shadowRadius=k.radius,te.shadowMapSize=k.mapSize,i.directionalShadow[h]=te,i.directionalShadowMap[h]=P,i.directionalShadowMatrix[h]=C.shadow.matrix,b++}i.directional[h]=W,h++}else if(C.isSpotLight){let W=e.get(C);W.position.setFromMatrixPosition(C.matrixWorld),W.color.copy(L).multiplyScalar($),W.distance=X,W.coneCos=Math.cos(C.angle),W.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),W.decay=C.decay,i.spot[x]=W;let k=C.shadow;if(C.map&&(i.spotLightMap[D]=C.map,D++,k.updateMatrices(C),C.castShadow&&T++),i.spotLightMatrix[x]=k.matrix,C.castShadow){let te=t.get(C);te.shadowIntensity=k.intensity,te.shadowBias=k.bias,te.shadowNormalBias=k.normalBias,te.shadowRadius=k.radius,te.shadowMapSize=k.mapSize,i.spotShadow[x]=te,i.spotShadowMap[x]=P,S++}x++}else if(C.isRectAreaLight){let W=e.get(C);W.color.copy(L).multiplyScalar($),W.halfWidth.set(C.width*.5,0,0),W.halfHeight.set(0,C.height*.5,0),i.rectArea[m]=W,m++}else if(C.isPointLight){let W=e.get(C);if(W.color.copy(C.color).multiplyScalar(C.intensity),W.distance=C.distance,W.decay=C.decay,C.castShadow){let k=C.shadow,te=t.get(C);te.shadowIntensity=k.intensity,te.shadowBias=k.bias,te.shadowNormalBias=k.normalBias,te.shadowRadius=k.radius,te.shadowMapSize=k.mapSize,te.shadowCameraNear=k.camera.near,te.shadowCameraFar=k.camera.far,i.pointShadow[g]=te,i.pointShadowMap[g]=P,i.pointShadowMatrix[g]=C.shadow.matrix,M++}i.point[g]=W,g++}else if(C.isHemisphereLight){let W=e.get(C);W.skyColor.copy(C.color).multiplyScalar($),W.groundColor.copy(C.groundColor).multiplyScalar($),i.hemi[p]=W,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=fe.LTC_FLOAT_1,i.rectAreaLTC2=fe.LTC_FLOAT_2):(i.rectAreaLTC1=fe.LTC_HALF_1,i.rectAreaLTC2=fe.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=f,i.ambient[2]=d;let y=i.hash;(y.directionalLength!==h||y.pointLength!==g||y.spotLength!==x||y.rectAreaLength!==m||y.hemiLength!==p||y.numDirectionalShadows!==b||y.numPointShadows!==M||y.numSpotShadows!==S||y.numSpotMaps!==D||y.numLightProbes!==I)&&(i.directional.length=h,i.spot.length=x,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=b,i.directionalShadowMap.length=b,i.pointShadow.length=M,i.pointShadowMap.length=M,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=b,i.pointShadowMatrix.length=M,i.spotLightMatrix.length=S+D-T,i.spotLightMap.length=D,i.numSpotLightShadowsWithMaps=T,i.numLightProbes=I,y.directionalLength=h,y.pointLength=g,y.spotLength=x,y.rectAreaLength=m,y.hemiLength=p,y.numDirectionalShadows=b,y.numPointShadows=M,y.numSpotShadows=S,y.numSpotMaps=D,y.numLightProbes=I,i.version=XD++)}function c(l,u){let f=0,d=0,h=0,g=0,x=0,m=u.matrixWorldInverse;for(let p=0,b=l.length;p<b;p++){let M=l[p];if(M.isDirectionalLight){let S=i.directional[f];S.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),f++}else if(M.isSpotLight){let S=i.spot[h];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(m),S.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(m),h++}else if(M.isRectAreaLight){let S=i.rectArea[g];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(m),o.identity(),s.copy(M.matrixWorld),s.premultiply(m),o.extractRotation(s),S.halfWidth.set(M.width*.5,0,0),S.halfHeight.set(0,M.height*.5,0),S.halfWidth.applyMatrix4(o),S.halfHeight.applyMatrix4(o),g++}else if(M.isPointLight){let S=i.point[d];S.position.setFromMatrixPosition(M.matrixWorld),S.position.applyMatrix4(m),d++}else if(M.isHemisphereLight){let S=i.hemi[x];S.direction.setFromMatrixPosition(M.matrixWorld),S.direction.transformDirection(m),x++}}}return{setup:a,setupView:c,state:i}}function Zx(n){let e=new ZD(n),t=[],i=[],r=[];function s(d){f.camera=d,t.length=0,i.length=0,r.length=0}function o(d){t.push(d)}function a(d){i.push(d)}function c(d){r.push(d)}function l(){e.setup(t)}function u(d){e.setupView(t,d)}let f={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:f,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function JD(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new Zx(n),e.set(r,[a])):s>=o.length?(a=new Zx(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var KD=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,QD=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,eR=[new B(1,0,0),new B(-1,0,0),new B(0,1,0),new B(0,-1,0),new B(0,0,1),new B(0,0,-1)],tR=[new B(0,-1,0),new B(0,-1,0),new B(0,0,1),new B(0,0,-1),new B(0,-1,0),new B(0,-1,0)],Jx=new Ot,Ja=new B,Jm=new B;function nR(n,e,t){let i=new Ra,r=new ut,s=new ut,o=new Ft,a=new Pu,c=new Lu,l={},u=t.maxTextureSize,f={[Qi]:xn,[xn]:Qi,[Ni]:Ni},d=new kn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new ut},radius:{value:4}},vertexShader:KD,fragmentShader:QD}),h=d.clone();h.defines.HORIZONTAL_PASS=1;let g=new _n;g.setAttribute("position",new fn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let x=new Kn(g,d),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Va;let p=this.type;this.render=function(T,I,y){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||T.length===0)return;this.type===X_&&(Ae("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Va);let w=n.getRenderTarget(),O=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),L=n.state;L.setBlending(Pi),L.buffers.depth.getReversed()===!0?L.buffers.color.setClear(0,0,0,0):L.buffers.color.setClear(1,1,1,1),L.buffers.depth.setTest(!0),L.setScissorTest(!1);let $=p!==this.type;$&&I.traverse(function(X){X.material&&(Array.isArray(X.material)?X.material.forEach(P=>P.needsUpdate=!0):X.material.needsUpdate=!0)});for(let X=0,P=T.length;X<P;X++){let W=T[X],k=W.shadow;if(k===void 0){Ae("WebGLShadowMap:",W,"has no shadow.");continue}if(k.autoUpdate===!1&&k.needsUpdate===!1)continue;r.copy(k.mapSize);let te=k.getFrameExtents();r.multiply(te),s.copy(k.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/te.x),r.x=s.x*te.x,k.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/te.y),r.y=s.y*te.y,k.mapSize.y=s.y));let ne=n.state.buffers.depth.getReversed();if(k.camera._reversedDepth=ne,k.map===null||$===!0){if(k.map!==null&&(k.map.depthTexture!==null&&(k.map.depthTexture.dispose(),k.map.depthTexture=null),k.map.dispose()),this.type===vo){if(W.isPointLight){Ae("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}k.map=new Fn(r.x,r.y,{format:Lr,type:Li,minFilter:en,magFilter:en,generateMipmaps:!1}),k.map.texture.name=W.name+".shadowMap",k.map.depthTexture=new tr(r.x,r.y,vi),k.map.depthTexture.name=W.name+".shadowMapDepth",k.map.depthTexture.format=Ai,k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=Jt,k.map.depthTexture.magFilter=Jt}else W.isPointLight?(k.map=new Od(r.x),k.map.depthTexture=new Ru(r.x,gi)):(k.map=new Fn(r.x,r.y),k.map.depthTexture=new tr(r.x,r.y,gi)),k.map.depthTexture.name=W.name+".shadowMap",k.map.depthTexture.format=Ai,this.type===Va?(k.map.depthTexture.compareFunction=ne?Rd:Dd,k.map.depthTexture.minFilter=en,k.map.depthTexture.magFilter=en):(k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=Jt,k.map.depthTexture.magFilter=Jt);k.camera.updateProjectionMatrix()}let he=k.map.isWebGLCubeRenderTarget?6:1;for(let be=0;be<he;be++){if(k.map.isWebGLCubeRenderTarget)n.setRenderTarget(k.map,be),n.clear();else{be===0&&(n.setRenderTarget(k.map),n.clear());let we=k.getViewport(be);o.set(s.x*we.x,s.y*we.y,s.x*we.z,s.y*we.w),L.viewport(o)}if(W.isPointLight){let we=k.camera,Qe=k.matrix,dt=W.distance||we.far;dt!==we.far&&(we.far=dt,we.updateProjectionMatrix()),Ja.setFromMatrixPosition(W.matrixWorld),we.position.copy(Ja),Jm.copy(we.position),Jm.add(eR[be]),we.up.copy(tR[be]),we.lookAt(Jm),we.updateMatrixWorld(),Qe.makeTranslation(-Ja.x,-Ja.y,-Ja.z),Jx.multiplyMatrices(we.projectionMatrix,we.matrixWorldInverse),k._frustum.setFromProjectionMatrix(Jx,we.coordinateSystem,we.reversedDepth)}else k.updateMatrices(W);i=k.getFrustum(),S(I,y,k.camera,W,this.type)}k.isPointLightShadow!==!0&&this.type===vo&&b(k,y),k.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(w,O,C)};function b(T,I){let y=e.update(x);d.defines.VSM_SAMPLES!==T.blurSamples&&(d.defines.VSM_SAMPLES=T.blurSamples,h.defines.VSM_SAMPLES=T.blurSamples,d.needsUpdate=!0,h.needsUpdate=!0),T.mapPass===null&&(T.mapPass=new Fn(r.x,r.y,{format:Lr,type:Li})),d.uniforms.shadow_pass.value=T.map.depthTexture,d.uniforms.resolution.value=T.mapSize,d.uniforms.radius.value=T.radius,n.setRenderTarget(T.mapPass),n.clear(),n.renderBufferDirect(I,null,y,d,x,null),h.uniforms.shadow_pass.value=T.mapPass.texture,h.uniforms.resolution.value=T.mapSize,h.uniforms.radius.value=T.radius,n.setRenderTarget(T.map),n.clear(),n.renderBufferDirect(I,null,y,h,x,null)}function M(T,I,y,w){let O=null,C=y.isPointLight===!0?T.customDistanceMaterial:T.customDepthMaterial;if(C!==void 0)O=C;else if(O=y.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let L=O.uuid,$=I.uuid,X=l[L];X===void 0&&(X={},l[L]=X);let P=X[$];P===void 0&&(P=O.clone(),X[$]=P,I.addEventListener("dispose",D)),O=P}if(O.visible=I.visible,O.wireframe=I.wireframe,w===vo?O.side=I.shadowSide!==null?I.shadowSide:I.side:O.side=I.shadowSide!==null?I.shadowSide:f[I.side],O.alphaMap=I.alphaMap,O.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,O.map=I.map,O.clipShadows=I.clipShadows,O.clippingPlanes=I.clippingPlanes,O.clipIntersection=I.clipIntersection,O.displacementMap=I.displacementMap,O.displacementScale=I.displacementScale,O.displacementBias=I.displacementBias,O.wireframeLinewidth=I.wireframeLinewidth,O.linewidth=I.linewidth,y.isPointLight===!0&&O.isMeshDistanceMaterial===!0){let L=n.properties.get(O);L.light=y}return O}function S(T,I,y,w,O){if(T.visible===!1)return;if(T.layers.test(I.layers)&&(T.isMesh||T.isLine||T.isPoints)&&(T.castShadow||T.receiveShadow&&O===vo)&&(!T.frustumCulled||i.intersectsObject(T))){T.modelViewMatrix.multiplyMatrices(y.matrixWorldInverse,T.matrixWorld);let $=e.update(T),X=T.material;if(Array.isArray(X)){let P=$.groups;for(let W=0,k=P.length;W<k;W++){let te=P[W],ne=X[te.materialIndex];if(ne&&ne.visible){let he=M(T,ne,w,O);T.onBeforeShadow(n,T,I,y,$,he,te),n.renderBufferDirect(y,null,$,he,T,te),T.onAfterShadow(n,T,I,y,$,he,te)}}}else if(X.visible){let P=M(T,X,w,O);T.onBeforeShadow(n,T,I,y,$,P,null),n.renderBufferDirect(y,null,$,P,T,null),T.onAfterShadow(n,T,I,y,$,P,null)}}let L=T.children;for(let $=0,X=L.length;$<X;$++)S(L[$],I,y,w,O)}function D(T){T.target.removeEventListener("dispose",D);for(let y in l){let w=l[y],O=T.target.uuid;O in w&&(w[O].dispose(),delete w[O])}}}function iR(n,e){function t(){let A=!1,se=new Ft,Y=null,ye=new Ft(0,0,0,0);return{setMask:function(le){Y!==le&&!A&&(n.colorMask(le,le,le,le),Y=le)},setLocked:function(le){A=le},setClear:function(le,Q,Se,Ve,kt){kt===!0&&(le*=Ve,Q*=Ve,Se*=Ve),se.set(le,Q,Se,Ve),ye.equals(se)===!1&&(n.clearColor(le,Q,Se,Ve),ye.copy(se))},reset:function(){A=!1,Y=null,ye.set(-1,0,0,0)}}}function i(){let A=!1,se=!1,Y=null,ye=null,le=null;return{setReversed:function(Q){if(se!==Q){let Se=e.get("EXT_clip_control");Q?Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.ZERO_TO_ONE_EXT):Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.NEGATIVE_ONE_TO_ONE_EXT),se=Q;let Ve=le;le=null,this.setClear(Ve)}},getReversed:function(){return se},setTest:function(Q){Q?ae(n.DEPTH_TEST):Re(n.DEPTH_TEST)},setMask:function(Q){Y!==Q&&!A&&(n.depthMask(Q),Y=Q)},setFunc:function(Q){if(se&&(Q=Cx[Q]),ye!==Q){switch(Q){case fu:n.depthFunc(n.NEVER);break;case hu:n.depthFunc(n.ALWAYS);break;case pu:n.depthFunc(n.LESS);break;case vs:n.depthFunc(n.LEQUAL);break;case mu:n.depthFunc(n.EQUAL);break;case gu:n.depthFunc(n.GEQUAL);break;case vu:n.depthFunc(n.GREATER);break;case yu:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ye=Q}},setLocked:function(Q){A=Q},setClear:function(Q){le!==Q&&(le=Q,se&&(Q=1-Q),n.clearDepth(Q))},reset:function(){A=!1,Y=null,ye=null,le=null,se=!1}}}function r(){let A=!1,se=null,Y=null,ye=null,le=null,Q=null,Se=null,Ve=null,kt=null;return{setTest:function(ht){A||(ht?ae(n.STENCIL_TEST):Re(n.STENCIL_TEST))},setMask:function(ht){se!==ht&&!A&&(n.stencilMask(ht),se=ht)},setFunc:function(ht,ki,yi){(Y!==ht||ye!==ki||le!==yi)&&(n.stencilFunc(ht,ki,yi),Y=ht,ye=ki,le=yi)},setOp:function(ht,ki,yi){(Q!==ht||Se!==ki||Ve!==yi)&&(n.stencilOp(ht,ki,yi),Q=ht,Se=ki,Ve=yi)},setLocked:function(ht){A=ht},setClear:function(ht){kt!==ht&&(n.clearStencil(ht),kt=ht)},reset:function(){A=!1,se=null,Y=null,ye=null,le=null,Q=null,Se=null,Ve=null,kt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},f={},d={},h=new WeakMap,g=[],x=null,m=!1,p=null,b=null,M=null,S=null,D=null,T=null,I=null,y=new it(0,0,0),w=0,O=!1,C=null,L=null,$=null,X=null,P=null,W=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),k=!1,te=0,ne=n.getParameter(n.VERSION);ne.indexOf("WebGL")!==-1?(te=parseFloat(/^WebGL (\d)/.exec(ne)[1]),k=te>=1):ne.indexOf("OpenGL ES")!==-1&&(te=parseFloat(/^OpenGL ES (\d)/.exec(ne)[1]),k=te>=2);let he=null,be={},we=n.getParameter(n.SCISSOR_BOX),Qe=n.getParameter(n.VIEWPORT),dt=new Ft().fromArray(we),ze=new Ft().fromArray(Qe);function K(A,se,Y,ye){let le=new Uint8Array(4),Q=n.createTexture();n.bindTexture(A,Q),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Se=0;Se<Y;Se++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(se,0,n.RGBA,1,1,ye,0,n.RGBA,n.UNSIGNED_BYTE,le):n.texImage2D(se+Se,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,le);return Q}let ge={};ge[n.TEXTURE_2D]=K(n.TEXTURE_2D,n.TEXTURE_2D,1),ge[n.TEXTURE_CUBE_MAP]=K(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),ge[n.TEXTURE_2D_ARRAY]=K(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),ge[n.TEXTURE_3D]=K(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ae(n.DEPTH_TEST),o.setFunc(vs),Wt(!1),Ct(xm),ae(n.CULL_FACE),Mt(Pi);function ae(A){u[A]!==!0&&(n.enable(A),u[A]=!0)}function Re(A){u[A]!==!1&&(n.disable(A),u[A]=!1)}function Be(A,se){return d[A]!==se?(n.bindFramebuffer(A,se),d[A]=se,A===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=se),A===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=se),!0):!1}function Pe(A,se){let Y=g,ye=!1;if(A){Y=h.get(se),Y===void 0&&(Y=[],h.set(se,Y));let le=A.textures;if(Y.length!==le.length||Y[0]!==n.COLOR_ATTACHMENT0){for(let Q=0,Se=le.length;Q<Se;Q++)Y[Q]=n.COLOR_ATTACHMENT0+Q;Y.length=le.length,ye=!0}}else Y[0]!==n.BACK&&(Y[0]=n.BACK,ye=!0);ye&&n.drawBuffers(Y)}function wt(A){return x!==A?(n.useProgram(A),x=A,!0):!1}let Ye={[Tr]:n.FUNC_ADD,[Z_]:n.FUNC_SUBTRACT,[J_]:n.FUNC_REVERSE_SUBTRACT};Ye[K_]=n.MIN,Ye[Q_]=n.MAX;let ft={[ex]:n.ZERO,[tx]:n.ONE,[nx]:n.SRC_COLOR,[uu]:n.SRC_ALPHA,[cx]:n.SRC_ALPHA_SATURATE,[ox]:n.DST_COLOR,[rx]:n.DST_ALPHA,[ix]:n.ONE_MINUS_SRC_COLOR,[du]:n.ONE_MINUS_SRC_ALPHA,[ax]:n.ONE_MINUS_DST_COLOR,[sx]:n.ONE_MINUS_DST_ALPHA,[lx]:n.CONSTANT_COLOR,[ux]:n.ONE_MINUS_CONSTANT_COLOR,[dx]:n.CONSTANT_ALPHA,[fx]:n.ONE_MINUS_CONSTANT_ALPHA};function Mt(A,se,Y,ye,le,Q,Se,Ve,kt,ht){if(A===Pi){m===!0&&(Re(n.BLEND),m=!1);return}if(m===!1&&(ae(n.BLEND),m=!0),A!==Y_){if(A!==p||ht!==O){if((b!==Tr||D!==Tr)&&(n.blendEquation(n.FUNC_ADD),b=Tr,D=Tr),ht)switch(A){case gs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Em:n.blendFunc(n.ONE,n.ONE);break;case bm:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Mm:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Ne("WebGLState: Invalid blending: ",A);break}else switch(A){case gs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Em:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case bm:Ne("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Mm:Ne("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Ne("WebGLState: Invalid blending: ",A);break}M=null,S=null,T=null,I=null,y.set(0,0,0),w=0,p=A,O=ht}return}le=le||se,Q=Q||Y,Se=Se||ye,(se!==b||le!==D)&&(n.blendEquationSeparate(Ye[se],Ye[le]),b=se,D=le),(Y!==M||ye!==S||Q!==T||Se!==I)&&(n.blendFuncSeparate(ft[Y],ft[ye],ft[Q],ft[Se]),M=Y,S=ye,T=Q,I=Se),(Ve.equals(y)===!1||kt!==w)&&(n.blendColor(Ve.r,Ve.g,Ve.b,kt),y.copy(Ve),w=kt),p=A,O=!1}function qe(A,se){A.side===Ni?Re(n.CULL_FACE):ae(n.CULL_FACE);let Y=A.side===xn;se&&(Y=!Y),Wt(Y),A.blending===gs&&A.transparent===!1?Mt(Pi):Mt(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),o.setFunc(A.depthFunc),o.setTest(A.depthTest),o.setMask(A.depthWrite),s.setMask(A.colorWrite);let ye=A.stencilWrite;a.setTest(ye),ye&&(a.setMask(A.stencilWriteMask),a.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),a.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),R(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?ae(n.SAMPLE_ALPHA_TO_COVERAGE):Re(n.SAMPLE_ALPHA_TO_COVERAGE)}function Wt(A){C!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),C=A)}function Ct(A){A!==$_?(ae(n.CULL_FACE),A!==L&&(A===xm?n.cullFace(n.BACK):A===q_?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Re(n.CULL_FACE),L=A}function wn(A){A!==$&&(k&&n.lineWidth(A),$=A)}function R(A,se,Y){A?(ae(n.POLYGON_OFFSET_FILL),(X!==se||P!==Y)&&(X=se,P=Y,o.getReversed()&&(se=-se),n.polygonOffset(se,Y))):Re(n.POLYGON_OFFSET_FILL)}function jt(A){A?ae(n.SCISSOR_TEST):Re(n.SCISSOR_TEST)}function Ze(A){A===void 0&&(A=n.TEXTURE0+W-1),he!==A&&(n.activeTexture(A),he=A)}function Et(A,se,Y){Y===void 0&&(he===null?Y=n.TEXTURE0+W-1:Y=he);let ye=be[Y];ye===void 0&&(ye={type:void 0,texture:void 0},be[Y]=ye),(ye.type!==A||ye.texture!==se)&&(he!==Y&&(n.activeTexture(Y),he=Y),n.bindTexture(A,se||ge[A]),ye.type=A,ye.texture=se)}function de(){let A=be[he];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function Dt(){try{n.compressedTexImage2D(...arguments)}catch(A){Ne("WebGLState:",A)}}function E(){try{n.compressedTexImage3D(...arguments)}catch(A){Ne("WebGLState:",A)}}function v(){try{n.texSubImage2D(...arguments)}catch(A){Ne("WebGLState:",A)}}function F(){try{n.texSubImage3D(...arguments)}catch(A){Ne("WebGLState:",A)}}function Z(){try{n.compressedTexSubImage2D(...arguments)}catch(A){Ne("WebGLState:",A)}}function ee(){try{n.compressedTexSubImage3D(...arguments)}catch(A){Ne("WebGLState:",A)}}function ie(){try{n.texStorage2D(...arguments)}catch(A){Ne("WebGLState:",A)}}function ue(){try{n.texStorage3D(...arguments)}catch(A){Ne("WebGLState:",A)}}function q(){try{n.texImage2D(...arguments)}catch(A){Ne("WebGLState:",A)}}function J(){try{n.texImage3D(...arguments)}catch(A){Ne("WebGLState:",A)}}function ve(A){return f[A]!==void 0?f[A]:n.getParameter(A)}function xe(A,se){f[A]!==se&&(n.pixelStorei(A,se),f[A]=se)}function ce(A){dt.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),dt.copy(A))}function re(A){ze.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),ze.copy(A))}function Oe(A,se){let Y=l.get(se);Y===void 0&&(Y=new WeakMap,l.set(se,Y));let ye=Y.get(A);ye===void 0&&(ye=n.getUniformBlockIndex(se,A.name),Y.set(A,ye))}function Ge(A,se){let ye=l.get(se).get(A);c.get(se)!==ye&&(n.uniformBlockBinding(se,ye,A.__bindingPointIndex),c.set(se,ye))}function rt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},f={},he=null,be={},d={},h=new WeakMap,g=[],x=null,m=!1,p=null,b=null,M=null,S=null,D=null,T=null,I=null,y=new it(0,0,0),w=0,O=!1,C=null,L=null,$=null,X=null,P=null,dt.set(0,0,n.canvas.width,n.canvas.height),ze.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ae,disable:Re,bindFramebuffer:Be,drawBuffers:Pe,useProgram:wt,setBlending:Mt,setMaterial:qe,setFlipSided:Wt,setCullFace:Ct,setLineWidth:wn,setPolygonOffset:R,setScissorTest:jt,activeTexture:Ze,bindTexture:Et,unbindTexture:de,compressedTexImage2D:Dt,compressedTexImage3D:E,texImage2D:q,texImage3D:J,pixelStorei:xe,getParameter:ve,updateUBOMapping:Oe,uniformBlockBinding:Ge,texStorage2D:ie,texStorage3D:ue,texSubImage2D:v,texSubImage3D:F,compressedTexSubImage2D:Z,compressedTexSubImage3D:ee,scissor:ce,viewport:re,reset:rt}}function rR(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new ut,u=new WeakMap,f=new Set,d,h=new WeakMap,g=!1;try{g=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function x(E,v){return g?new OffscreenCanvas(E,v):Ma("canvas")}function m(E,v,F){let Z=1,ee=Dt(E);if((ee.width>F||ee.height>F)&&(Z=F/Math.max(ee.width,ee.height)),Z<1)if(typeof HTMLImageElement<"u"&&E instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&E instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&E instanceof ImageBitmap||typeof VideoFrame<"u"&&E instanceof VideoFrame){let ie=Math.floor(Z*ee.width),ue=Math.floor(Z*ee.height);d===void 0&&(d=x(ie,ue));let q=v?x(ie,ue):d;return q.width=ie,q.height=ue,q.getContext("2d").drawImage(E,0,0,ie,ue),Ae("WebGLRenderer: Texture has been resized from ("+ee.width+"x"+ee.height+") to ("+ie+"x"+ue+")."),q}else return"data"in E&&Ae("WebGLRenderer: Image in DataTexture is too big ("+ee.width+"x"+ee.height+")."),E;return E}function p(E){return E.generateMipmaps}function b(E){n.generateMipmap(E)}function M(E){return E.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:E.isWebGL3DRenderTarget?n.TEXTURE_3D:E.isWebGLArrayRenderTarget||E.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function S(E,v,F,Z,ee,ie=!1){if(E!==null){if(n[E]!==void 0)return n[E];Ae("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+E+"'")}let ue;Z&&(ue=e.get("EXT_texture_norm16"),ue||Ae("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let q=v;if(v===n.RED&&(F===n.FLOAT&&(q=n.R32F),F===n.HALF_FLOAT&&(q=n.R16F),F===n.UNSIGNED_BYTE&&(q=n.R8),F===n.UNSIGNED_SHORT&&ue&&(q=ue.R16_EXT),F===n.SHORT&&ue&&(q=ue.R16_SNORM_EXT)),v===n.RED_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.R8UI),F===n.UNSIGNED_SHORT&&(q=n.R16UI),F===n.UNSIGNED_INT&&(q=n.R32UI),F===n.BYTE&&(q=n.R8I),F===n.SHORT&&(q=n.R16I),F===n.INT&&(q=n.R32I)),v===n.RG&&(F===n.FLOAT&&(q=n.RG32F),F===n.HALF_FLOAT&&(q=n.RG16F),F===n.UNSIGNED_BYTE&&(q=n.RG8),F===n.UNSIGNED_SHORT&&ue&&(q=ue.RG16_EXT),F===n.SHORT&&ue&&(q=ue.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RG8UI),F===n.UNSIGNED_SHORT&&(q=n.RG16UI),F===n.UNSIGNED_INT&&(q=n.RG32UI),F===n.BYTE&&(q=n.RG8I),F===n.SHORT&&(q=n.RG16I),F===n.INT&&(q=n.RG32I)),v===n.RGB_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RGB8UI),F===n.UNSIGNED_SHORT&&(q=n.RGB16UI),F===n.UNSIGNED_INT&&(q=n.RGB32UI),F===n.BYTE&&(q=n.RGB8I),F===n.SHORT&&(q=n.RGB16I),F===n.INT&&(q=n.RGB32I)),v===n.RGBA_INTEGER&&(F===n.UNSIGNED_BYTE&&(q=n.RGBA8UI),F===n.UNSIGNED_SHORT&&(q=n.RGBA16UI),F===n.UNSIGNED_INT&&(q=n.RGBA32UI),F===n.BYTE&&(q=n.RGBA8I),F===n.SHORT&&(q=n.RGBA16I),F===n.INT&&(q=n.RGBA32I)),v===n.RGB&&(F===n.UNSIGNED_SHORT&&ue&&(q=ue.RGB16_EXT),F===n.SHORT&&ue&&(q=ue.RGB16_SNORM_EXT),F===n.UNSIGNED_INT_5_9_9_9_REV&&(q=n.RGB9_E5),F===n.UNSIGNED_INT_10F_11F_11F_REV&&(q=n.R11F_G11F_B10F)),v===n.RGBA){let J=ie?Ea:Je.getTransfer(ee);F===n.FLOAT&&(q=n.RGBA32F),F===n.HALF_FLOAT&&(q=n.RGBA16F),F===n.UNSIGNED_BYTE&&(q=J===at?n.SRGB8_ALPHA8:n.RGBA8),F===n.UNSIGNED_SHORT&&ue&&(q=ue.RGBA16_EXT),F===n.SHORT&&ue&&(q=ue.RGBA16_SNORM_EXT),F===n.UNSIGNED_SHORT_4_4_4_4&&(q=n.RGBA4),F===n.UNSIGNED_SHORT_5_5_5_1&&(q=n.RGB5_A1)}return(q===n.R16F||q===n.R32F||q===n.RG16F||q===n.RG32F||q===n.RGBA16F||q===n.RGBA32F)&&e.get("EXT_color_buffer_float"),q}function D(E,v){let F;return E?v===null||v===gi||v===_o?F=n.DEPTH24_STENCIL8:v===vi?F=n.DEPTH32F_STENCIL8:v===yo&&(F=n.DEPTH24_STENCIL8,Ae("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===gi||v===_o?F=n.DEPTH_COMPONENT24:v===vi?F=n.DEPTH_COMPONENT32F:v===yo&&(F=n.DEPTH_COMPONENT16),F}function T(E,v){return p(E)===!0||E.isFramebufferTexture&&E.minFilter!==Jt&&E.minFilter!==en?Math.log2(Math.max(v.width,v.height))+1:E.mipmaps!==void 0&&E.mipmaps.length>0?E.mipmaps.length:E.isCompressedTexture&&Array.isArray(E.image)?v.mipmaps.length:1}function I(E){let v=E.target;v.removeEventListener("dispose",I),w(v),v.isVideoTexture&&u.delete(v),v.isHTMLTexture&&f.delete(v)}function y(E){let v=E.target;v.removeEventListener("dispose",y),C(v)}function w(E){let v=i.get(E);if(v.__webglInit===void 0)return;let F=E.source,Z=h.get(F);if(Z){let ee=Z[v.__cacheKey];ee.usedTimes--,ee.usedTimes===0&&O(E),Object.keys(Z).length===0&&h.delete(F)}i.remove(E)}function O(E){let v=i.get(E);n.deleteTexture(v.__webglTexture);let F=E.source,Z=h.get(F);delete Z[v.__cacheKey],o.memory.textures--}function C(E){let v=i.get(E);if(E.depthTexture&&(E.depthTexture.dispose(),i.remove(E.depthTexture)),E.isWebGLCubeRenderTarget)for(let Z=0;Z<6;Z++){if(Array.isArray(v.__webglFramebuffer[Z]))for(let ee=0;ee<v.__webglFramebuffer[Z].length;ee++)n.deleteFramebuffer(v.__webglFramebuffer[Z][ee]);else n.deleteFramebuffer(v.__webglFramebuffer[Z]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[Z])}else{if(Array.isArray(v.__webglFramebuffer))for(let Z=0;Z<v.__webglFramebuffer.length;Z++)n.deleteFramebuffer(v.__webglFramebuffer[Z]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let Z=0;Z<v.__webglColorRenderbuffer.length;Z++)v.__webglColorRenderbuffer[Z]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[Z]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let F=E.textures;for(let Z=0,ee=F.length;Z<ee;Z++){let ie=i.get(F[Z]);ie.__webglTexture&&(n.deleteTexture(ie.__webglTexture),o.memory.textures--),i.remove(F[Z])}i.remove(E)}let L=0;function $(){L=0}function X(){return L}function P(E){L=E}function W(){let E=L;return E>=r.maxTextures&&Ae("WebGLTextures: Trying to use "+E+" texture units while this GPU supports only "+r.maxTextures),L+=1,E}function k(E){let v=[];return v.push(E.wrapS),v.push(E.wrapT),v.push(E.wrapR||0),v.push(E.magFilter),v.push(E.minFilter),v.push(E.anisotropy),v.push(E.internalFormat),v.push(E.format),v.push(E.type),v.push(E.generateMipmaps),v.push(E.premultiplyAlpha),v.push(E.flipY),v.push(E.unpackAlignment),v.push(E.colorSpace),v.join()}function te(E,v){let F=i.get(E);if(E.isVideoTexture&&Et(E),E.isRenderTargetTexture===!1&&E.isExternalTexture!==!0&&E.version>0&&F.__version!==E.version){let Z=E.image;if(Z===null)Ae("WebGLRenderer: Texture marked for update but no image data found.");else if(Z.complete===!1)Ae("WebGLRenderer: Texture marked for update but image is incomplete");else{Re(F,E,v);return}}else E.isExternalTexture&&(F.__webglTexture=E.sourceTexture?E.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,F.__webglTexture,n.TEXTURE0+v)}function ne(E,v){let F=i.get(E);if(E.isRenderTargetTexture===!1&&E.version>0&&F.__version!==E.version){Re(F,E,v);return}else E.isExternalTexture&&(F.__webglTexture=E.sourceTexture?E.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,F.__webglTexture,n.TEXTURE0+v)}function he(E,v){let F=i.get(E);if(E.isRenderTargetTexture===!1&&E.version>0&&F.__version!==E.version){Re(F,E,v);return}t.bindTexture(n.TEXTURE_3D,F.__webglTexture,n.TEXTURE0+v)}function be(E,v){let F=i.get(E);if(E.isCubeDepthTexture!==!0&&E.version>0&&F.__version!==E.version){Be(F,E,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,F.__webglTexture,n.TEXTURE0+v)}let we={[_u]:n.REPEAT,[Ii]:n.CLAMP_TO_EDGE,[xu]:n.MIRRORED_REPEAT},Qe={[Jt]:n.NEAREST,[mx]:n.NEAREST_MIPMAP_NEAREST,[za]:n.NEAREST_MIPMAP_LINEAR,[en]:n.LINEAR,[qu]:n.LINEAR_MIPMAP_NEAREST,[Nr]:n.LINEAR_MIPMAP_LINEAR},dt={[yx]:n.NEVER,[Mx]:n.ALWAYS,[_x]:n.LESS,[Dd]:n.LEQUAL,[xx]:n.EQUAL,[Rd]:n.GEQUAL,[Ex]:n.GREATER,[bx]:n.NOTEQUAL};function ze(E,v){if(v.type===vi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===en||v.magFilter===qu||v.magFilter===za||v.magFilter===Nr||v.minFilter===en||v.minFilter===qu||v.minFilter===za||v.minFilter===Nr)&&Ae("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(E,n.TEXTURE_WRAP_S,we[v.wrapS]),n.texParameteri(E,n.TEXTURE_WRAP_T,we[v.wrapT]),(E===n.TEXTURE_3D||E===n.TEXTURE_2D_ARRAY)&&n.texParameteri(E,n.TEXTURE_WRAP_R,we[v.wrapR]),n.texParameteri(E,n.TEXTURE_MAG_FILTER,Qe[v.magFilter]),n.texParameteri(E,n.TEXTURE_MIN_FILTER,Qe[v.minFilter]),v.compareFunction&&(n.texParameteri(E,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(E,n.TEXTURE_COMPARE_FUNC,dt[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===Jt||v.minFilter!==za&&v.minFilter!==Nr||v.type===vi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let F=e.get("EXT_texture_filter_anisotropic");n.texParameterf(E,F.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function K(E,v){let F=!1;E.__webglInit===void 0&&(E.__webglInit=!0,v.addEventListener("dispose",I));let Z=v.source,ee=h.get(Z);ee===void 0&&(ee={},h.set(Z,ee));let ie=k(v);if(ie!==E.__cacheKey){ee[ie]===void 0&&(ee[ie]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,F=!0),ee[ie].usedTimes++;let ue=ee[E.__cacheKey];ue!==void 0&&(ee[E.__cacheKey].usedTimes--,ue.usedTimes===0&&O(v)),E.__cacheKey=ie,E.__webglTexture=ee[ie].texture}return F}function ge(E,v,F){return Math.floor(Math.floor(E/F)/v)}function ae(E,v,F,Z){let ie=E.updateRanges;if(ie.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,F,Z,v.data);else{ie.sort((xe,ce)=>xe.start-ce.start);let ue=0;for(let xe=1;xe<ie.length;xe++){let ce=ie[ue],re=ie[xe],Oe=ce.start+ce.count,Ge=ge(re.start,v.width,4),rt=ge(ce.start,v.width,4);re.start<=Oe+1&&Ge===rt&&ge(re.start+re.count-1,v.width,4)===Ge?ce.count=Math.max(ce.count,re.start+re.count-ce.start):(++ue,ie[ue]=re)}ie.length=ue+1;let q=t.getParameter(n.UNPACK_ROW_LENGTH),J=t.getParameter(n.UNPACK_SKIP_PIXELS),ve=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let xe=0,ce=ie.length;xe<ce;xe++){let re=ie[xe],Oe=Math.floor(re.start/4),Ge=Math.ceil(re.count/4),rt=Oe%v.width,A=Math.floor(Oe/v.width),se=Ge,Y=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,rt),t.pixelStorei(n.UNPACK_SKIP_ROWS,A),t.texSubImage2D(n.TEXTURE_2D,0,rt,A,se,Y,F,Z,v.data)}E.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,q),t.pixelStorei(n.UNPACK_SKIP_PIXELS,J),t.pixelStorei(n.UNPACK_SKIP_ROWS,ve)}}function Re(E,v,F){let Z=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(Z=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(Z=n.TEXTURE_3D);let ee=K(E,v),ie=v.source;t.bindTexture(Z,E.__webglTexture,n.TEXTURE0+F);let ue=i.get(ie);if(ie.version!==ue.__version||ee===!0){if(t.activeTexture(n.TEXTURE0+F),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let Y=Je.getPrimaries(Je.workingColorSpace),ye=v.colorSpace===nr?null:Je.getPrimaries(v.colorSpace),le=v.colorSpace===nr||Y===ye?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,le)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let J=m(v.image,!1,r.maxTextureSize);J=de(v,J);let ve=s.convert(v.format,v.colorSpace),xe=s.convert(v.type),ce=S(v.internalFormat,ve,xe,v.normalized,v.colorSpace,v.isVideoTexture);ze(Z,v);let re,Oe=v.mipmaps,Ge=v.isVideoTexture!==!0,rt=ue.__version===void 0||ee===!0,A=ie.dataReady,se=T(v,J);if(v.isDepthTexture)ce=D(v.format===Pr,v.type),rt&&(Ge?t.texStorage2D(n.TEXTURE_2D,1,ce,J.width,J.height):t.texImage2D(n.TEXTURE_2D,0,ce,J.width,J.height,0,ve,xe,null));else if(v.isDataTexture)if(Oe.length>0){Ge&&rt&&t.texStorage2D(n.TEXTURE_2D,se,ce,Oe[0].width,Oe[0].height);for(let Y=0,ye=Oe.length;Y<ye;Y++)re=Oe[Y],Ge?A&&t.texSubImage2D(n.TEXTURE_2D,Y,0,0,re.width,re.height,ve,xe,re.data):t.texImage2D(n.TEXTURE_2D,Y,ce,re.width,re.height,0,ve,xe,re.data);v.generateMipmaps=!1}else Ge?(rt&&t.texStorage2D(n.TEXTURE_2D,se,ce,J.width,J.height),A&&ae(v,J,ve,xe)):t.texImage2D(n.TEXTURE_2D,0,ce,J.width,J.height,0,ve,xe,J.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){Ge&&rt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,se,ce,Oe[0].width,Oe[0].height,J.depth);for(let Y=0,ye=Oe.length;Y<ye;Y++)if(re=Oe[Y],v.format!==Qn)if(ve!==null)if(Ge){if(A)if(v.layerUpdates.size>0){let le=jm(re.width,re.height,v.format,v.type);for(let Q of v.layerUpdates){let Se=re.data.subarray(Q*le/re.data.BYTES_PER_ELEMENT,(Q+1)*le/re.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,Y,0,0,Q,re.width,re.height,1,ve,Se)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,Y,0,0,0,re.width,re.height,J.depth,ve,re.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,Y,ce,re.width,re.height,J.depth,0,re.data,0,0);else Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Ge?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,Y,0,0,0,re.width,re.height,J.depth,ve,xe,re.data):t.texImage3D(n.TEXTURE_2D_ARRAY,Y,ce,re.width,re.height,J.depth,0,ve,xe,re.data)}else{Ge&&rt&&t.texStorage2D(n.TEXTURE_2D,se,ce,Oe[0].width,Oe[0].height);for(let Y=0,ye=Oe.length;Y<ye;Y++)re=Oe[Y],v.format!==Qn?ve!==null?Ge?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,Y,0,0,re.width,re.height,ve,re.data):t.compressedTexImage2D(n.TEXTURE_2D,Y,ce,re.width,re.height,0,re.data):Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Ge?A&&t.texSubImage2D(n.TEXTURE_2D,Y,0,0,re.width,re.height,ve,xe,re.data):t.texImage2D(n.TEXTURE_2D,Y,ce,re.width,re.height,0,ve,xe,re.data)}else if(v.isDataArrayTexture)if(Ge){if(rt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,se,ce,J.width,J.height,J.depth),A)if(v.layerUpdates.size>0){let Y=jm(J.width,J.height,v.format,v.type);for(let ye of v.layerUpdates){let le=J.data.subarray(ye*Y/J.data.BYTES_PER_ELEMENT,(ye+1)*Y/J.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,ye,J.width,J.height,1,ve,xe,le)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,J.width,J.height,J.depth,ve,xe,J.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,ce,J.width,J.height,J.depth,0,ve,xe,J.data);else if(v.isData3DTexture)Ge?(rt&&t.texStorage3D(n.TEXTURE_3D,se,ce,J.width,J.height,J.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,J.width,J.height,J.depth,ve,xe,J.data)):t.texImage3D(n.TEXTURE_3D,0,ce,J.width,J.height,J.depth,0,ve,xe,J.data);else if(v.isFramebufferTexture){if(rt)if(Ge)t.texStorage2D(n.TEXTURE_2D,se,ce,J.width,J.height);else{let Y=J.width,ye=J.height;for(let le=0;le<se;le++)t.texImage2D(n.TEXTURE_2D,le,ce,Y,ye,0,ve,xe,null),Y>>=1,ye>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let Y=n.canvas;if(Y.hasAttribute("layoutsubtree")||Y.setAttribute("layoutsubtree","true"),J.parentNode!==Y){Y.appendChild(J),f.add(v),Y.onpaint=Ve=>{let kt=Ve.changedElements;for(let ht of f)kt.includes(ht.image)&&(ht.needsUpdate=!0)},Y.requestPaint();return}let ye=0,le=n.RGBA,Q=n.RGBA,Se=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,ye,le,Q,Se,J),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(Oe.length>0){if(Ge&&rt){let Y=Dt(Oe[0]);t.texStorage2D(n.TEXTURE_2D,se,ce,Y.width,Y.height)}for(let Y=0,ye=Oe.length;Y<ye;Y++)re=Oe[Y],Ge?A&&t.texSubImage2D(n.TEXTURE_2D,Y,0,0,ve,xe,re):t.texImage2D(n.TEXTURE_2D,Y,ce,ve,xe,re);v.generateMipmaps=!1}else if(Ge){if(rt){let Y=Dt(J);t.texStorage2D(n.TEXTURE_2D,se,ce,Y.width,Y.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ve,xe,J)}else t.texImage2D(n.TEXTURE_2D,0,ce,ve,xe,J);p(v)&&b(Z),ue.__version=ie.version,v.onUpdate&&v.onUpdate(v)}E.__version=v.version}function Be(E,v,F){if(v.image.length!==6)return;let Z=K(E,v),ee=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,E.__webglTexture,n.TEXTURE0+F);let ie=i.get(ee);if(ee.version!==ie.__version||Z===!0){t.activeTexture(n.TEXTURE0+F);let ue=Je.getPrimaries(Je.workingColorSpace),q=v.colorSpace===nr?null:Je.getPrimaries(v.colorSpace),J=v.colorSpace===nr||ue===q?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,J);let ve=v.isCompressedTexture||v.image[0].isCompressedTexture,xe=v.image[0]&&v.image[0].isDataTexture,ce=[];for(let Q=0;Q<6;Q++)!ve&&!xe?ce[Q]=m(v.image[Q],!0,r.maxCubemapSize):ce[Q]=xe?v.image[Q].image:v.image[Q],ce[Q]=de(v,ce[Q]);let re=ce[0],Oe=s.convert(v.format,v.colorSpace),Ge=s.convert(v.type),rt=S(v.internalFormat,Oe,Ge,v.normalized,v.colorSpace),A=v.isVideoTexture!==!0,se=ie.__version===void 0||Z===!0,Y=ee.dataReady,ye=T(v,re);ze(n.TEXTURE_CUBE_MAP,v);let le;if(ve){A&&se&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ye,rt,re.width,re.height);for(let Q=0;Q<6;Q++){le=ce[Q].mipmaps;for(let Se=0;Se<le.length;Se++){let Ve=le[Se];v.format!==Qn?Oe!==null?A?Y&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se,0,0,Ve.width,Ve.height,Oe,Ve.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se,rt,Ve.width,Ve.height,0,Ve.data):Ae("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):A?Y&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se,0,0,Ve.width,Ve.height,Oe,Ge,Ve.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se,rt,Ve.width,Ve.height,0,Oe,Ge,Ve.data)}}}else{if(le=v.mipmaps,A&&se){le.length>0&&ye++;let Q=Dt(ce[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ye,rt,Q.width,Q.height)}for(let Q=0;Q<6;Q++)if(xe){A?Y&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,0,0,0,ce[Q].width,ce[Q].height,Oe,Ge,ce[Q].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,0,rt,ce[Q].width,ce[Q].height,0,Oe,Ge,ce[Q].data);for(let Se=0;Se<le.length;Se++){let kt=le[Se].image[Q].image;A?Y&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se+1,0,0,kt.width,kt.height,Oe,Ge,kt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se+1,rt,kt.width,kt.height,0,Oe,Ge,kt.data)}}else{A?Y&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,0,0,0,Oe,Ge,ce[Q]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,0,rt,Oe,Ge,ce[Q]);for(let Se=0;Se<le.length;Se++){let Ve=le[Se];A?Y&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se+1,0,0,Oe,Ge,Ve.image[Q]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Q,Se+1,rt,Oe,Ge,Ve.image[Q])}}}p(v)&&b(n.TEXTURE_CUBE_MAP),ie.__version=ee.version,v.onUpdate&&v.onUpdate(v)}E.__version=v.version}function Pe(E,v,F,Z,ee,ie){let ue=s.convert(F.format,F.colorSpace),q=s.convert(F.type),J=S(F.internalFormat,ue,q,F.normalized,F.colorSpace),ve=i.get(v),xe=i.get(F);if(xe.__renderTarget=v,!ve.__hasExternalTextures){let ce=Math.max(1,v.width>>ie),re=Math.max(1,v.height>>ie);ee===n.TEXTURE_3D||ee===n.TEXTURE_2D_ARRAY?t.texImage3D(ee,ie,J,ce,re,v.depth,0,ue,q,null):t.texImage2D(ee,ie,J,ce,re,0,ue,q,null)}t.bindFramebuffer(n.FRAMEBUFFER,E),Ze(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Z,ee,xe.__webglTexture,0,jt(v)):(ee===n.TEXTURE_2D||ee>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&ee<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,Z,ee,xe.__webglTexture,ie),t.bindFramebuffer(n.FRAMEBUFFER,null)}function wt(E,v,F){if(n.bindRenderbuffer(n.RENDERBUFFER,E),v.depthBuffer){let Z=v.depthTexture,ee=Z&&Z.isDepthTexture?Z.type:null,ie=D(v.stencilBuffer,ee),ue=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Ze(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,jt(v),ie,v.width,v.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,jt(v),ie,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,ie,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ue,n.RENDERBUFFER,E)}else{let Z=v.textures;for(let ee=0;ee<Z.length;ee++){let ie=Z[ee],ue=s.convert(ie.format,ie.colorSpace),q=s.convert(ie.type),J=S(ie.internalFormat,ue,q,ie.normalized,ie.colorSpace);Ze(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,jt(v),J,v.width,v.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,jt(v),J,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,J,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ye(E,v,F){let Z=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,E),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let ee=i.get(v.depthTexture);if(ee.__renderTarget=v,(!ee.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),Z){if(ee.__webglInit===void 0&&(ee.__webglInit=!0,v.depthTexture.addEventListener("dispose",I)),ee.__webglTexture===void 0){ee.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,ee.__webglTexture),ze(n.TEXTURE_CUBE_MAP,v.depthTexture);let ve=s.convert(v.depthTexture.format),xe=s.convert(v.depthTexture.type),ce;v.depthTexture.format===Ai?ce=n.DEPTH_COMPONENT24:v.depthTexture.format===Pr&&(ce=n.DEPTH24_STENCIL8);for(let re=0;re<6;re++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+re,0,ce,v.width,v.height,0,ve,xe,null)}}else te(v.depthTexture,0);let ie=ee.__webglTexture,ue=jt(v),q=Z?n.TEXTURE_CUBE_MAP_POSITIVE_X+F:n.TEXTURE_2D,J=v.depthTexture.format===Pr?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Ai)Ze(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,J,q,ie,0,ue):n.framebufferTexture2D(n.FRAMEBUFFER,J,q,ie,0);else if(v.depthTexture.format===Pr)Ze(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,J,q,ie,0,ue):n.framebufferTexture2D(n.FRAMEBUFFER,J,q,ie,0);else throw new Error("Unknown depthTexture format")}function ft(E){let v=i.get(E),F=E.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==E.depthTexture){let Z=E.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),Z){let ee=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,Z.removeEventListener("dispose",ee)};Z.addEventListener("dispose",ee),v.__depthDisposeCallback=ee}v.__boundDepthTexture=Z}if(E.depthTexture&&!v.__autoAllocateDepthBuffer)if(F)for(let Z=0;Z<6;Z++)Ye(v.__webglFramebuffer[Z],E,Z);else{let Z=E.texture.mipmaps;Z&&Z.length>0?Ye(v.__webglFramebuffer[0],E,0):Ye(v.__webglFramebuffer,E,0)}else if(F){v.__webglDepthbuffer=[];for(let Z=0;Z<6;Z++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[Z]),v.__webglDepthbuffer[Z]===void 0)v.__webglDepthbuffer[Z]=n.createRenderbuffer(),wt(v.__webglDepthbuffer[Z],E,!1);else{let ee=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ie=v.__webglDepthbuffer[Z];n.bindRenderbuffer(n.RENDERBUFFER,ie),n.framebufferRenderbuffer(n.FRAMEBUFFER,ee,n.RENDERBUFFER,ie)}}else{let Z=E.texture.mipmaps;if(Z&&Z.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),wt(v.__webglDepthbuffer,E,!1);else{let ee=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ie=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,ie),n.framebufferRenderbuffer(n.FRAMEBUFFER,ee,n.RENDERBUFFER,ie)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Mt(E,v,F){let Z=i.get(E);v!==void 0&&Pe(Z.__webglFramebuffer,E,E.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),F!==void 0&&ft(E)}function qe(E){let v=E.texture,F=i.get(E),Z=i.get(v);E.addEventListener("dispose",y);let ee=E.textures,ie=E.isWebGLCubeRenderTarget===!0,ue=ee.length>1;if(ue||(Z.__webglTexture===void 0&&(Z.__webglTexture=n.createTexture()),Z.__version=v.version,o.memory.textures++),ie){F.__webglFramebuffer=[];for(let q=0;q<6;q++)if(v.mipmaps&&v.mipmaps.length>0){F.__webglFramebuffer[q]=[];for(let J=0;J<v.mipmaps.length;J++)F.__webglFramebuffer[q][J]=n.createFramebuffer()}else F.__webglFramebuffer[q]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){F.__webglFramebuffer=[];for(let q=0;q<v.mipmaps.length;q++)F.__webglFramebuffer[q]=n.createFramebuffer()}else F.__webglFramebuffer=n.createFramebuffer();if(ue)for(let q=0,J=ee.length;q<J;q++){let ve=i.get(ee[q]);ve.__webglTexture===void 0&&(ve.__webglTexture=n.createTexture(),o.memory.textures++)}if(E.samples>0&&Ze(E)===!1){F.__webglMultisampledFramebuffer=n.createFramebuffer(),F.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,F.__webglMultisampledFramebuffer);for(let q=0;q<ee.length;q++){let J=ee[q];F.__webglColorRenderbuffer[q]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,F.__webglColorRenderbuffer[q]);let ve=s.convert(J.format,J.colorSpace),xe=s.convert(J.type),ce=S(J.internalFormat,ve,xe,J.normalized,J.colorSpace,E.isXRRenderTarget===!0),re=jt(E);n.renderbufferStorageMultisample(n.RENDERBUFFER,re,ce,E.width,E.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+q,n.RENDERBUFFER,F.__webglColorRenderbuffer[q])}n.bindRenderbuffer(n.RENDERBUFFER,null),E.depthBuffer&&(F.__webglDepthRenderbuffer=n.createRenderbuffer(),wt(F.__webglDepthRenderbuffer,E,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(ie){t.bindTexture(n.TEXTURE_CUBE_MAP,Z.__webglTexture),ze(n.TEXTURE_CUBE_MAP,v);for(let q=0;q<6;q++)if(v.mipmaps&&v.mipmaps.length>0)for(let J=0;J<v.mipmaps.length;J++)Pe(F.__webglFramebuffer[q][J],E,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+q,J);else Pe(F.__webglFramebuffer[q],E,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+q,0);p(v)&&b(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ue){for(let q=0,J=ee.length;q<J;q++){let ve=ee[q],xe=i.get(ve),ce=n.TEXTURE_2D;(E.isWebGL3DRenderTarget||E.isWebGLArrayRenderTarget)&&(ce=E.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(ce,xe.__webglTexture),ze(ce,ve),Pe(F.__webglFramebuffer,E,ve,n.COLOR_ATTACHMENT0+q,ce,0),p(ve)&&b(ce)}t.unbindTexture()}else{let q=n.TEXTURE_2D;if((E.isWebGL3DRenderTarget||E.isWebGLArrayRenderTarget)&&(q=E.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(q,Z.__webglTexture),ze(q,v),v.mipmaps&&v.mipmaps.length>0)for(let J=0;J<v.mipmaps.length;J++)Pe(F.__webglFramebuffer[J],E,v,n.COLOR_ATTACHMENT0,q,J);else Pe(F.__webglFramebuffer,E,v,n.COLOR_ATTACHMENT0,q,0);p(v)&&b(q),t.unbindTexture()}E.depthBuffer&&ft(E)}function Wt(E){let v=E.textures;for(let F=0,Z=v.length;F<Z;F++){let ee=v[F];if(p(ee)){let ie=M(E),ue=i.get(ee).__webglTexture;t.bindTexture(ie,ue),b(ie),t.unbindTexture()}}}let Ct=[],wn=[];function R(E){if(E.samples>0){if(Ze(E)===!1){let v=E.textures,F=E.width,Z=E.height,ee=n.COLOR_BUFFER_BIT,ie=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ue=i.get(E),q=v.length>1;if(q)for(let ve=0;ve<v.length;ve++)t.bindFramebuffer(n.FRAMEBUFFER,ue.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ue.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ue.__webglMultisampledFramebuffer);let J=E.texture.mipmaps;J&&J.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ue.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ue.__webglFramebuffer);for(let ve=0;ve<v.length;ve++){if(E.resolveDepthBuffer&&(E.depthBuffer&&(ee|=n.DEPTH_BUFFER_BIT),E.stencilBuffer&&E.resolveStencilBuffer&&(ee|=n.STENCIL_BUFFER_BIT)),q){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ue.__webglColorRenderbuffer[ve]);let xe=i.get(v[ve]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,xe,0)}n.blitFramebuffer(0,0,F,Z,0,0,F,Z,ee,n.NEAREST),c===!0&&(Ct.length=0,wn.length=0,Ct.push(n.COLOR_ATTACHMENT0+ve),E.depthBuffer&&E.resolveDepthBuffer===!1&&(Ct.push(ie),wn.push(ie),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,wn)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Ct))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),q)for(let ve=0;ve<v.length;ve++){t.bindFramebuffer(n.FRAMEBUFFER,ue.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.RENDERBUFFER,ue.__webglColorRenderbuffer[ve]);let xe=i.get(v[ve]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ue.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+ve,n.TEXTURE_2D,xe,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ue.__webglMultisampledFramebuffer)}else if(E.depthBuffer&&E.resolveDepthBuffer===!1&&c){let v=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function jt(E){return Math.min(r.maxSamples,E.samples)}function Ze(E){let v=i.get(E);return E.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function Et(E){let v=o.render.frame;u.get(E)!==v&&(u.set(E,v),E.update())}function de(E,v){let F=E.colorSpace,Z=E.format,ee=E.type;return E.isCompressedTexture===!0||E.isVideoTexture===!0||F!==xa&&F!==nr&&(Je.getTransfer(F)===at?(Z!==Qn||ee!==Bn)&&Ae("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Ne("WebGLTextures: Unsupported texture color space:",F)),v}function Dt(E){return typeof HTMLImageElement<"u"&&E instanceof HTMLImageElement?(l.width=E.naturalWidth||E.width,l.height=E.naturalHeight||E.height):typeof VideoFrame<"u"&&E instanceof VideoFrame?(l.width=E.displayWidth,l.height=E.displayHeight):(l.width=E.width,l.height=E.height),l}this.allocateTextureUnit=W,this.resetTextureUnits=$,this.getTextureUnits=X,this.setTextureUnits=P,this.setTexture2D=te,this.setTexture2DArray=ne,this.setTexture3D=he,this.setTextureCube=be,this.rebindTextures=Mt,this.setupRenderTarget=qe,this.updateRenderTargetMipmap=Wt,this.updateMultisampleRenderTarget=R,this.setupDepthRenderbuffer=ft,this.setupFrameBufferTexture=Pe,this.useMultisampledRTT=Ze,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function sR(n,e){function t(i,r=nr){let s,o=Je.getTransfer(r);if(i===Bn)return n.UNSIGNED_BYTE;if(i===Yu)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Zu)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Lm)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Om)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Nm)return n.BYTE;if(i===Pm)return n.SHORT;if(i===yo)return n.UNSIGNED_SHORT;if(i===Xu)return n.INT;if(i===gi)return n.UNSIGNED_INT;if(i===vi)return n.FLOAT;if(i===Li)return n.HALF_FLOAT;if(i===Fm)return n.ALPHA;if(i===km)return n.RGB;if(i===Qn)return n.RGBA;if(i===Ai)return n.DEPTH_COMPONENT;if(i===Pr)return n.DEPTH_STENCIL;if(i===Um)return n.RED;if(i===Ju)return n.RED_INTEGER;if(i===Lr)return n.RG;if(i===Ku)return n.RG_INTEGER;if(i===Qu)return n.RGBA_INTEGER;if(i===Ga||i===Wa||i===ja||i===$a)if(o===at)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Ga)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Wa)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===ja)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===$a)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Ga)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Wa)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===ja)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===$a)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===ed||i===td||i===nd||i===id)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===ed)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===td)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===nd)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===id)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===rd||i===sd||i===od||i===ad||i===cd||i===qa||i===ld)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===rd||i===sd)return o===at?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===od)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===ad)return s.COMPRESSED_R11_EAC;if(i===cd)return s.COMPRESSED_SIGNED_R11_EAC;if(i===qa)return s.COMPRESSED_RG11_EAC;if(i===ld)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===ud||i===dd||i===fd||i===hd||i===pd||i===md||i===gd||i===vd||i===yd||i===_d||i===xd||i===Ed||i===bd||i===Md)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===ud)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===dd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===fd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===hd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===pd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===md)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===gd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===vd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===yd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===_d)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===xd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Ed)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===bd)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Md)return o===at?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Sd||i===Td||i===wd)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Sd)return o===at?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Td)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===wd)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Cd||i===Id||i===Xa||i===Ad)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Cd)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Id)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Xa)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Ad)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===_o?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var oR=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,aR=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,sg=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Oa(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new kn({vertexShader:oR,fragmentShader:aR,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Kn(new Fa(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},og=class extends Di{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,f=null,d=null,h=null,g=null,x=typeof XRWebGLBinding<"u",m=new sg,p={},b=t.getContextAttributes(),M=null,S=null,D=[],T=[],I=new ut,y=null,w=new dn;w.viewport=new Ft;let O=new dn;O.viewport=new Ft;let C=[w,O],L=new Wu,$=null,X=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(K){let ge=D[K];return ge===void 0&&(ge=new fo,D[K]=ge),ge.getTargetRaySpace()},this.getControllerGrip=function(K){let ge=D[K];return ge===void 0&&(ge=new fo,D[K]=ge),ge.getGripSpace()},this.getHand=function(K){let ge=D[K];return ge===void 0&&(ge=new fo,D[K]=ge),ge.getHandSpace()};function P(K){let ge=T.indexOf(K.inputSource);if(ge===-1)return;let ae=D[ge];ae!==void 0&&(ae.update(K.inputSource,K.frame,l||o),ae.dispatchEvent({type:K.type,data:K.inputSource}))}function W(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",W),r.removeEventListener("inputsourceschange",k);for(let K=0;K<D.length;K++){let ge=T[K];ge!==null&&(T[K]=null,D[K].disconnect(ge))}$=null,X=null,m.reset();for(let K in p)delete p[K];e.setRenderTarget(M),h=null,d=null,f=null,r=null,S=null,ze.stop(),i.isPresenting=!1,e.setPixelRatio(y),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(K){s=K,i.isPresenting===!0&&Ae("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(K){a=K,i.isPresenting===!0&&Ae("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(K){l=K},this.getBaseLayer=function(){return d!==null?d:h},this.getBinding=function(){return f===null&&x&&(f=new XRWebGLBinding(r,t)),f},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=async function(K){if(r=K,r!==null){if(M=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",W),r.addEventListener("inputsourceschange",k),b.xrCompatible!==!0&&await t.makeXRCompatible(),y=e.getPixelRatio(),e.getSize(I),x&&"createProjectionLayer"in XRWebGLBinding.prototype){let ae=null,Re=null,Be=null;b.depth&&(Be=b.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ae=b.stencil?Pr:Ai,Re=b.stencil?_o:gi);let Pe={colorFormat:t.RGBA8,depthFormat:Be,scaleFactor:s};f=this.getBinding(),d=f.createProjectionLayer(Pe),r.updateRenderState({layers:[d]}),e.setPixelRatio(1),e.setSize(d.textureWidth,d.textureHeight,!1),S=new Fn(d.textureWidth,d.textureHeight,{format:Qn,type:Bn,depthTexture:new tr(d.textureWidth,d.textureHeight,Re,void 0,void 0,void 0,void 0,void 0,void 0,ae),stencilBuffer:b.stencil,colorSpace:e.outputColorSpace,samples:b.antialias?4:0,resolveDepthBuffer:d.ignoreDepthValues===!1,resolveStencilBuffer:d.ignoreDepthValues===!1})}else{let ae={antialias:b.antialias,alpha:!0,depth:b.depth,stencil:b.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ae),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new Fn(h.framebufferWidth,h.framebufferHeight,{format:Qn,type:Bn,colorSpace:e.outputColorSpace,stencilBuffer:b.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),ze.setContext(r),ze.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function k(K){for(let ge=0;ge<K.removed.length;ge++){let ae=K.removed[ge],Re=T.indexOf(ae);Re>=0&&(T[Re]=null,D[Re].disconnect(ae))}for(let ge=0;ge<K.added.length;ge++){let ae=K.added[ge],Re=T.indexOf(ae);if(Re===-1){for(let Pe=0;Pe<D.length;Pe++)if(Pe>=T.length){T.push(ae),Re=Pe;break}else if(T[Pe]===null){T[Pe]=ae,Re=Pe;break}if(Re===-1)break}let Be=D[Re];Be&&Be.connect(ae)}}let te=new B,ne=new B;function he(K,ge,ae){te.setFromMatrixPosition(ge.matrixWorld),ne.setFromMatrixPosition(ae.matrixWorld);let Re=te.distanceTo(ne),Be=ge.projectionMatrix.elements,Pe=ae.projectionMatrix.elements,wt=Be[14]/(Be[10]-1),Ye=Be[14]/(Be[10]+1),ft=(Be[9]+1)/Be[5],Mt=(Be[9]-1)/Be[5],qe=(Be[8]-1)/Be[0],Wt=(Pe[8]+1)/Pe[0],Ct=wt*qe,wn=wt*Wt,R=Re/(-qe+Wt),jt=R*-qe;if(ge.matrixWorld.decompose(K.position,K.quaternion,K.scale),K.translateX(jt),K.translateZ(R),K.matrixWorld.compose(K.position,K.quaternion,K.scale),K.matrixWorldInverse.copy(K.matrixWorld).invert(),Be[10]===-1)K.projectionMatrix.copy(ge.projectionMatrix),K.projectionMatrixInverse.copy(ge.projectionMatrixInverse);else{let Ze=wt+R,Et=Ye+R,de=Ct-jt,Dt=wn+(Re-jt),E=ft*Ye/Et*Ze,v=Mt*Ye/Et*Ze;K.projectionMatrix.makePerspective(de,Dt,E,v,Ze,Et),K.projectionMatrixInverse.copy(K.projectionMatrix).invert()}}function be(K,ge){ge===null?K.matrixWorld.copy(K.matrix):K.matrixWorld.multiplyMatrices(ge.matrixWorld,K.matrix),K.matrixWorldInverse.copy(K.matrixWorld).invert()}this.updateCamera=function(K){if(r===null)return;let ge=K.near,ae=K.far;m.texture!==null&&(m.depthNear>0&&(ge=m.depthNear),m.depthFar>0&&(ae=m.depthFar)),L.near=O.near=w.near=ge,L.far=O.far=w.far=ae,($!==L.near||X!==L.far)&&(r.updateRenderState({depthNear:L.near,depthFar:L.far}),$=L.near,X=L.far),L.layers.mask=K.layers.mask|6,w.layers.mask=L.layers.mask&-5,O.layers.mask=L.layers.mask&-3;let Re=K.parent,Be=L.cameras;be(L,Re);for(let Pe=0;Pe<Be.length;Pe++)be(Be[Pe],Re);Be.length===2?he(L,w,O):L.projectionMatrix.copy(w.projectionMatrix),we(K,L,Re)};function we(K,ge,ae){ae===null?K.matrix.copy(ge.matrixWorld):(K.matrix.copy(ae.matrixWorld),K.matrix.invert(),K.matrix.multiply(ge.matrixWorld)),K.matrix.decompose(K.position,K.quaternion,K.scale),K.updateMatrixWorld(!0),K.projectionMatrix.copy(ge.projectionMatrix),K.projectionMatrixInverse.copy(ge.projectionMatrixInverse),K.isPerspectiveCamera&&(K.fov=Mu*2*Math.atan(1/K.projectionMatrix.elements[5]),K.zoom=1)}this.getCamera=function(){return L},this.getFoveation=function(){if(!(d===null&&h===null))return c},this.setFoveation=function(K){c=K,d!==null&&(d.fixedFoveation=K),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=K)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(L)},this.getCameraTexture=function(K){return p[K]};let Qe=null;function dt(K,ge){if(u=ge.getViewerPose(l||o),g=ge,u!==null){let ae=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Re=!1;ae.length!==L.cameras.length&&(L.cameras.length=0,Re=!0);for(let Ye=0;Ye<ae.length;Ye++){let ft=ae[Ye],Mt=null;if(h!==null)Mt=h.getViewport(ft);else{let Wt=f.getViewSubImage(d,ft);Mt=Wt.viewport,Ye===0&&(e.setRenderTargetTextures(S,Wt.colorTexture,Wt.depthStencilTexture),e.setRenderTarget(S))}let qe=C[Ye];qe===void 0&&(qe=new dn,qe.layers.enable(Ye),qe.viewport=new Ft,C[Ye]=qe),qe.matrix.fromArray(ft.transform.matrix),qe.matrix.decompose(qe.position,qe.quaternion,qe.scale),qe.projectionMatrix.fromArray(ft.projectionMatrix),qe.projectionMatrixInverse.copy(qe.projectionMatrix).invert(),qe.viewport.set(Mt.x,Mt.y,Mt.width,Mt.height),Ye===0&&(L.matrix.copy(qe.matrix),L.matrix.decompose(L.position,L.quaternion,L.scale)),Re===!0&&L.cameras.push(qe)}let Be=r.enabledFeatures;if(Be&&Be.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&x){f=i.getBinding();let Ye=f.getDepthInformation(ae[0]);Ye&&Ye.isValid&&Ye.texture&&m.init(Ye,r.renderState)}if(Be&&Be.includes("camera-access")&&x){e.state.unbindTexture(),f=i.getBinding();for(let Ye=0;Ye<ae.length;Ye++){let ft=ae[Ye].camera;if(ft){let Mt=p[ft];Mt||(Mt=new Oa,p[ft]=Mt);let qe=f.getCameraImage(ft);Mt.sourceTexture=qe}}}}for(let ae=0;ae<D.length;ae++){let Re=T[ae],Be=D[ae];Re!==null&&Be!==void 0&&Be.update(Re,ge,l||o)}Qe&&Qe(K,ge),ge.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:ge}),g=null}let ze=new Kx;ze.setAnimationLoop(dt),this.setAnimationLoop=function(K){Qe=K},this.dispose=function(){}}},cR=new Ot,rE=new Ue;rE.set(-1,0,0,0,1,0,0,0,1);function lR(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,zm(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,b,M,S){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),f(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),d(m,p),p.isMeshPhysicalMaterial&&h(m,p,S)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),x(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,b,M):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===xn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===xn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let b=e.get(p),M=b.envMap,S=b.envMapRotation;M&&(m.envMap.value=M,m.envMapRotation.value.setFromMatrix4(cR.makeRotationFromEuler(S)).transpose(),M.isCubeTexture&&M.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(rE),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,b,M){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*b,m.scale.value=M*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function f(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function d(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,b){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===xn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=b.texture,m.transmissionSamplerSize.value.set(b.width,b.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function x(m,p){let b=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(b.matrixWorld),m.nearDistance.value=b.shadow.camera.near,m.farDistance.value=b.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function uR(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(b,M){let S=M.program;i.uniformBlockBinding(b,S)}function l(b,M){let S=r[b.id];S===void 0&&(g(b),S=u(b),r[b.id]=S,b.addEventListener("dispose",m));let D=M.program;i.updateUBOMapping(b,D);let T=e.render.frame;s[b.id]!==T&&(d(b),s[b.id]=T)}function u(b){let M=f();b.__bindingPointIndex=M;let S=n.createBuffer(),D=b.__size,T=b.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,D,T),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,M,S),S}function f(){for(let b=0;b<a;b++)if(o.indexOf(b)===-1)return o.push(b),b;return Ne("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function d(b){let M=r[b.id],S=b.uniforms,D=b.__cache;n.bindBuffer(n.UNIFORM_BUFFER,M);for(let T=0,I=S.length;T<I;T++){let y=Array.isArray(S[T])?S[T]:[S[T]];for(let w=0,O=y.length;w<O;w++){let C=y[w];if(h(C,T,w,D)===!0){let L=C.__offset,$=Array.isArray(C.value)?C.value:[C.value],X=0;for(let P=0;P<$.length;P++){let W=$[P],k=x(W);typeof W=="number"||typeof W=="boolean"?(C.__data[0]=W,n.bufferSubData(n.UNIFORM_BUFFER,L+X,C.__data)):W.isMatrix3?(C.__data[0]=W.elements[0],C.__data[1]=W.elements[1],C.__data[2]=W.elements[2],C.__data[3]=0,C.__data[4]=W.elements[3],C.__data[5]=W.elements[4],C.__data[6]=W.elements[5],C.__data[7]=0,C.__data[8]=W.elements[6],C.__data[9]=W.elements[7],C.__data[10]=W.elements[8],C.__data[11]=0):ArrayBuffer.isView(W)?C.__data.set(new W.constructor(W.buffer,W.byteOffset,C.__data.length)):(W.toArray(C.__data,X),X+=k.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,L,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(b,M,S,D){let T=b.value,I=M+"_"+S;if(D[I]===void 0)return typeof T=="number"||typeof T=="boolean"?D[I]=T:ArrayBuffer.isView(T)?D[I]=T.slice():D[I]=T.clone(),!0;{let y=D[I];if(typeof T=="number"||typeof T=="boolean"){if(y!==T)return D[I]=T,!0}else{if(ArrayBuffer.isView(T))return!0;if(y.equals(T)===!1)return y.copy(T),!0}}return!1}function g(b){let M=b.uniforms,S=0,D=16;for(let I=0,y=M.length;I<y;I++){let w=Array.isArray(M[I])?M[I]:[M[I]];for(let O=0,C=w.length;O<C;O++){let L=w[O],$=Array.isArray(L.value)?L.value:[L.value];for(let X=0,P=$.length;X<P;X++){let W=$[X],k=x(W),te=S%D,ne=te%k.boundary,he=te+ne;S+=ne,he!==0&&D-he<k.storage&&(S+=D-he),L.__data=new Float32Array(k.storage/Float32Array.BYTES_PER_ELEMENT),L.__offset=S,S+=k.storage}}}let T=S%D;return T>0&&(S+=D-T),b.__size=S,b.__cache={},this}function x(b){let M={boundary:0,storage:0};return typeof b=="number"||typeof b=="boolean"?(M.boundary=4,M.storage=4):b.isVector2?(M.boundary=8,M.storage=8):b.isVector3||b.isColor?(M.boundary=16,M.storage=12):b.isVector4?(M.boundary=16,M.storage=16):b.isMatrix3?(M.boundary=48,M.storage=48):b.isMatrix4?(M.boundary=64,M.storage=64):b.isTexture?Ae("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(b)?(M.boundary=16,M.storage=b.byteLength):Ae("WebGLRenderer: Unsupported uniform value type.",b),M}function m(b){let M=b.target;M.removeEventListener("dispose",m);let S=o.indexOf(M.__bindingPointIndex);o.splice(S,1),n.deleteBuffer(r[M.id]),delete r[M.id],delete s[M.id]}function p(){for(let b in r)n.deleteBuffer(r[b]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var dR=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),Oi=null;function fR(){return Oi===null&&(Oi=new Cu(dR,16,16,Lr,Li),Oi.name="DFG_LUT",Oi.minFilter=en,Oi.magFilter=en,Oi.wrapS=Ii,Oi.wrapT=Ii,Oi.generateMipmaps=!1,Oi.needsUpdate=!0),Oi}var Fd=class{constructor(e={}){let{canvas:t=Sx(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:f=!1,reversedDepthBuffer:d=!1,outputBufferType:h=Bn}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let x=h,m=new Set([Qu,Ku,Ju]),p=new Set([Bn,gi,yo,_o,Yu,Zu]),b=new Uint32Array(4),M=new Int32Array(4),S=new B,D=null,T=null,I=[],y=[],w=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=mi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let O=this,C=!1,L=null;this._outputColorSpace=On;let $=0,X=0,P=null,W=-1,k=null,te=new Ft,ne=new Ft,he=null,be=new it(0),we=0,Qe=t.width,dt=t.height,ze=1,K=null,ge=null,ae=new Ft(0,0,Qe,dt),Re=new Ft(0,0,Qe,dt),Be=!1,Pe=new Ra,wt=!1,Ye=!1,ft=new Ot,Mt=new B,qe=new Ft,Wt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Ct=!1;function wn(){return P===null?ze:1}let R=i;function jt(_,N){return t.getContext(_,N)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:f};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Q,!1),t.addEventListener("webglcontextrestored",Se,!1),t.addEventListener("webglcontextcreationerror",Ve,!1),R===null){let N="webgl2";if(R=jt(N,_),R===null)throw jt(N)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw Ne("WebGLRenderer: "+_.message),_}let Ze,Et,de,Dt,E,v,F,Z,ee,ie,ue,q,J,ve,xe,ce,re,Oe,Ge,rt,A,se,Y;function ye(){Ze=new _1(R),Ze.init(),A=new sR(R,Ze),Et=new d1(R,Ze,e,A),de=new iR(R,Ze),Et.reversedDepthBuffer&&d&&de.buffers.depth.setReversed(!0),Dt=new b1(R),E=new GD,v=new rR(R,Ze,de,E,Et,A,Dt),F=new y1(O),Z=new wC(R),se=new l1(R,Z),ee=new x1(R,Z,Dt,se),ie=new S1(R,ee,Z,se,Dt),Oe=new M1(R,Et,v),xe=new f1(E),ue=new zD(O,F,Ze,Et,se,xe),q=new lR(O,E),J=new jD,ve=new JD(Ze),re=new c1(O,F,de,ie,g,c),ce=new nR(O,ie,Et),Y=new uR(R,Dt,Et,de),Ge=new u1(R,Ze,Dt),rt=new E1(R,Ze,Dt),Dt.programs=ue.programs,O.capabilities=Et,O.extensions=Ze,O.properties=E,O.renderLists=J,O.shadowMap=ce,O.state=de,O.info=Dt}ye(),x!==Bn&&(w=new w1(x,t.width,t.height,r,s));let le=new og(O,R);this.xr=le,this.getContext=function(){return R},this.getContextAttributes=function(){return R.getContextAttributes()},this.forceContextLoss=function(){let _=Ze.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=Ze.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return ze},this.setPixelRatio=function(_){_!==void 0&&(ze=_,this.setSize(Qe,dt,!1))},this.getSize=function(_){return _.set(Qe,dt)},this.setSize=function(_,N,G=!0){if(le.isPresenting){Ae("WebGLRenderer: Can't change size while VR device is presenting.");return}Qe=_,dt=N,t.width=Math.floor(_*ze),t.height=Math.floor(N*ze),G===!0&&(t.style.width=_+"px",t.style.height=N+"px"),w!==null&&w.setSize(t.width,t.height),this.setViewport(0,0,_,N)},this.getDrawingBufferSize=function(_){return _.set(Qe*ze,dt*ze).floor()},this.setDrawingBufferSize=function(_,N,G){Qe=_,dt=N,ze=G,t.width=Math.floor(_*G),t.height=Math.floor(N*G),this.setViewport(0,0,_,N)},this.setEffects=function(_){if(x===Bn){Ne("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let N=0;N<_.length;N++)if(_[N].isOutputPass===!0){Ae("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}w.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(te)},this.getViewport=function(_){return _.copy(ae)},this.setViewport=function(_,N,G,V){_.isVector4?ae.set(_.x,_.y,_.z,_.w):ae.set(_,N,G,V),de.viewport(te.copy(ae).multiplyScalar(ze).round())},this.getScissor=function(_){return _.copy(Re)},this.setScissor=function(_,N,G,V){_.isVector4?Re.set(_.x,_.y,_.z,_.w):Re.set(_,N,G,V),de.scissor(ne.copy(Re).multiplyScalar(ze).round())},this.getScissorTest=function(){return Be},this.setScissorTest=function(_){de.setScissorTest(Be=_)},this.setOpaqueSort=function(_){K=_},this.setTransparentSort=function(_){ge=_},this.getClearColor=function(_){return _.copy(re.getClearColor())},this.setClearColor=function(){re.setClearColor(...arguments)},this.getClearAlpha=function(){return re.getClearAlpha()},this.setClearAlpha=function(){re.setClearAlpha(...arguments)},this.clear=function(_=!0,N=!0,G=!0){let V=0;if(_){let H=!1;if(P!==null){let me=P.texture.format;H=m.has(me)}if(H){let me=P.texture.type,Ee=p.has(me),pe=re.getClearColor(),Me=re.getClearAlpha(),Te=pe.r,He=pe.g,je=pe.b;Ee?(b[0]=Te,b[1]=He,b[2]=je,b[3]=Me,R.clearBufferuiv(R.COLOR,0,b)):(M[0]=Te,M[1]=He,M[2]=je,M[3]=Me,R.clearBufferiv(R.COLOR,0,M))}else V|=R.COLOR_BUFFER_BIT}N&&(V|=R.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),G&&(V|=R.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),V!==0&&R.clear(V)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(_){_.setRenderer(this),L=_},this.dispose=function(){t.removeEventListener("webglcontextlost",Q,!1),t.removeEventListener("webglcontextrestored",Se,!1),t.removeEventListener("webglcontextcreationerror",Ve,!1),re.dispose(),J.dispose(),ve.dispose(),E.dispose(),F.dispose(),ie.dispose(),se.dispose(),Y.dispose(),ue.dispose(),le.dispose(),le.removeEventListener("sessionstart",lg),le.removeEventListener("sessionend",ug),Fr.stop()};function Q(_){_.preventDefault(),Hm("WebGLRenderer: Context Lost."),C=!0}function Se(){Hm("WebGLRenderer: Context Restored."),C=!1;let _=Dt.autoReset,N=ce.enabled,G=ce.autoUpdate,V=ce.needsUpdate,H=ce.type;ye(),Dt.autoReset=_,ce.enabled=N,ce.autoUpdate=G,ce.needsUpdate=V,ce.type=H}function Ve(_){Ne("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function kt(_){let N=_.target;N.removeEventListener("dispose",kt),ht(N)}function ht(_){ki(_),E.remove(_)}function ki(_){let N=E.get(_).programs;N!==void 0&&(N.forEach(function(G){ue.releaseProgram(G)}),_.isShaderMaterial&&ue.releaseShaderCache(_))}this.renderBufferDirect=function(_,N,G,V,H,me){N===null&&(N=Wt);let Ee=H.isMesh&&H.matrixWorld.determinant()<0,pe=lE(_,N,G,V,H);de.setMaterial(V,Ee);let Me=G.index,Te=1;if(V.wireframe===!0){if(Me=ee.getWireframeAttribute(G),Me===void 0)return;Te=2}let He=G.drawRange,je=G.attributes.position,Ce=He.start*Te,pt=(He.start+He.count)*Te;me!==null&&(Ce=Math.max(Ce,me.start*Te),pt=Math.min(pt,(me.start+me.count)*Te)),Me!==null?(Ce=Math.max(Ce,0),pt=Math.min(pt,Me.count)):je!=null&&(Ce=Math.max(Ce,0),pt=Math.min(pt,je.count));let Ut=pt-Ce;if(Ut<0||Ut===1/0)return;se.setup(H,V,pe,G,Me);let Rt,_t=Ge;if(Me!==null&&(Rt=Z.get(Me),_t=rt,_t.setIndex(Rt)),H.isMesh)V.wireframe===!0?(de.setLineWidth(V.wireframeLinewidth*wn()),_t.setMode(R.LINES)):_t.setMode(R.TRIANGLES);else if(H.isLine){let tn=V.linewidth;tn===void 0&&(tn=1),de.setLineWidth(tn*wn()),H.isLineSegments?_t.setMode(R.LINES):H.isLineLoop?_t.setMode(R.LINE_LOOP):_t.setMode(R.LINE_STRIP)}else H.isPoints?_t.setMode(R.POINTS):H.isSprite&&_t.setMode(R.TRIANGLES);if(H.isBatchedMesh)if(Ze.get("WEBGL_multi_draw"))_t.renderMultiDraw(H._multiDrawStarts,H._multiDrawCounts,H._multiDrawCount);else{let tn=H._multiDrawStarts,_e=H._multiDrawCounts,Cn=H._multiDrawCount,et=Me?Z.get(Me).bytesPerElement:1,Vn=E.get(V).currentProgram.getUniforms();for(let _i=0;_i<Cn;_i++)Vn.setValue(R,"_gl_DrawID",_i),_t.render(tn[_i]/et,_e[_i])}else if(H.isInstancedMesh)_t.renderInstances(Ce,Ut,H.count);else if(G.isInstancedBufferGeometry){let tn=G._maxInstanceCount!==void 0?G._maxInstanceCount:1/0,_e=Math.min(G.instanceCount,tn);_t.renderInstances(Ce,Ut,_e)}else _t.render(Ce,Ut)};function yi(_,N,G){_.transparent===!0&&_.side===Ni&&_.forceSinglePass===!1?(_.side=xn,_.needsUpdate=!0,ec(_,N,G),_.side=Qi,_.needsUpdate=!0,ec(_,N,G),_.side=Ni):ec(_,N,G)}this.compile=function(_,N,G=null){G===null&&(G=_),T=ve.get(G),T.init(N),y.push(T),G.traverseVisible(function(H){H.isLight&&H.layers.test(N.layers)&&(T.pushLight(H),H.castShadow&&T.pushShadow(H))}),_!==G&&_.traverseVisible(function(H){H.isLight&&H.layers.test(N.layers)&&(T.pushLight(H),H.castShadow&&T.pushShadow(H))}),T.setupLights();let V=new Set;return _.traverse(function(H){if(!(H.isMesh||H.isPoints||H.isLine||H.isSprite))return;let me=H.material;if(me)if(Array.isArray(me))for(let Ee=0;Ee<me.length;Ee++){let pe=me[Ee];yi(pe,G,H),V.add(pe)}else yi(me,G,H),V.add(me)}),T=y.pop(),V},this.compileAsync=function(_,N,G=null){let V=this.compile(_,N,G);return new Promise(H=>{function me(){if(V.forEach(function(Ee){E.get(Ee).currentProgram.isReady()&&V.delete(Ee)}),V.size===0){H(_);return}setTimeout(me,10)}Ze.get("KHR_parallel_shader_compile")!==null?me():setTimeout(me,10)})};let Jd=null;function aE(_){Jd&&Jd(_)}function lg(){Fr.stop()}function ug(){Fr.start()}let Fr=new Kx;Fr.setAnimationLoop(aE),typeof self<"u"&&Fr.setContext(self),this.setAnimationLoop=function(_){Jd=_,le.setAnimationLoop(_),_===null?Fr.stop():Fr.start()},le.addEventListener("sessionstart",lg),le.addEventListener("sessionend",ug),this.render=function(_,N){if(N!==void 0&&N.isCamera!==!0){Ne("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(C===!0)return;L!==null&&L.renderStart(_,N);let G=le.enabled===!0&&le.isPresenting===!0,V=w!==null&&(P===null||G)&&w.begin(O,P);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),le.enabled===!0&&le.isPresenting===!0&&(w===null||w.isCompositing()===!1)&&(le.cameraAutoUpdate===!0&&le.updateCamera(N),N=le.getCamera()),_.isScene===!0&&_.onBeforeRender(O,_,N,P),T=ve.get(_,y.length),T.init(N),T.state.textureUnits=v.getTextureUnits(),y.push(T),ft.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),Pe.setFromProjectionMatrix(ft,pi,N.reversedDepth),Ye=this.localClippingEnabled,wt=xe.init(this.clippingPlanes,Ye),D=J.get(_,I.length),D.init(),I.push(D),le.enabled===!0&&le.isPresenting===!0){let Ee=O.xr.getDepthSensingMesh();Ee!==null&&Kd(Ee,N,-1/0,O.sortObjects)}Kd(_,N,0,O.sortObjects),D.finish(),O.sortObjects===!0&&D.sort(K,ge),Ct=le.enabled===!1||le.isPresenting===!1||le.hasDepthSensing()===!1,Ct&&re.addToRenderList(D,_),this.info.render.frame++,wt===!0&&xe.beginShadows();let H=T.state.shadowsArray;if(ce.render(H,_,N),wt===!0&&xe.endShadows(),this.info.autoReset===!0&&this.info.reset(),(V&&w.hasRenderPass())===!1){let Ee=D.opaque,pe=D.transmissive;if(T.setupLights(),N.isArrayCamera){let Me=N.cameras;if(pe.length>0)for(let Te=0,He=Me.length;Te<He;Te++){let je=Me[Te];fg(Ee,pe,_,je)}Ct&&re.render(_);for(let Te=0,He=Me.length;Te<He;Te++){let je=Me[Te];dg(D,_,je,je.viewport)}}else pe.length>0&&fg(Ee,pe,_,N),Ct&&re.render(_),dg(D,_,N)}P!==null&&X===0&&(v.updateMultisampleRenderTarget(P),v.updateRenderTargetMipmap(P)),V&&w.end(O),_.isScene===!0&&_.onAfterRender(O,_,N),se.resetDefaultState(),W=-1,k=null,y.pop(),y.length>0?(T=y[y.length-1],v.setTextureUnits(T.state.textureUnits),wt===!0&&xe.setGlobalState(O.clippingPlanes,T.state.camera)):T=null,I.pop(),I.length>0?D=I[I.length-1]:D=null,L!==null&&L.renderEnd()};function Kd(_,N,G,V){if(_.visible===!1)return;if(_.layers.test(N.layers)){if(_.isGroup)G=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(N);else if(_.isLightProbeGrid)T.pushLightProbeGrid(_);else if(_.isLight)T.pushLight(_),_.castShadow&&T.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||Pe.intersectsSprite(_)){V&&qe.setFromMatrixPosition(_.matrixWorld).applyMatrix4(ft);let Ee=ie.update(_),pe=_.material;pe.visible&&D.push(_,Ee,pe,G,qe.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||Pe.intersectsObject(_))){let Ee=ie.update(_),pe=_.material;if(V&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),qe.copy(_.boundingSphere.center)):(Ee.boundingSphere===null&&Ee.computeBoundingSphere(),qe.copy(Ee.boundingSphere.center)),qe.applyMatrix4(_.matrixWorld).applyMatrix4(ft)),Array.isArray(pe)){let Me=Ee.groups;for(let Te=0,He=Me.length;Te<He;Te++){let je=Me[Te],Ce=pe[je.materialIndex];Ce&&Ce.visible&&D.push(_,Ee,Ce,G,qe.z,je)}}else pe.visible&&D.push(_,Ee,pe,G,qe.z,null)}}let me=_.children;for(let Ee=0,pe=me.length;Ee<pe;Ee++)Kd(me[Ee],N,G,V)}function dg(_,N,G,V){let{opaque:H,transmissive:me,transparent:Ee}=_;T.setupLightsView(G),wt===!0&&xe.setGlobalState(O.clippingPlanes,G),V&&de.viewport(te.copy(V)),H.length>0&&Qa(H,N,G),me.length>0&&Qa(me,N,G),Ee.length>0&&Qa(Ee,N,G),de.buffers.depth.setTest(!0),de.buffers.depth.setMask(!0),de.buffers.color.setMask(!0),de.setPolygonOffset(!1)}function fg(_,N,G,V){if((G.isScene===!0?G.overrideMaterial:null)!==null)return;if(T.state.transmissionRenderTarget[V.id]===void 0){let Ce=Ze.has("EXT_color_buffer_half_float")||Ze.has("EXT_color_buffer_float");T.state.transmissionRenderTarget[V.id]=new Fn(1,1,{generateMipmaps:!0,type:Ce?Li:Bn,minFilter:Nr,samples:Math.max(4,Et.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:Je.workingColorSpace})}let me=T.state.transmissionRenderTarget[V.id],Ee=V.viewport||te;me.setSize(Ee.z*O.transmissionResolutionScale,Ee.w*O.transmissionResolutionScale);let pe=O.getRenderTarget(),Me=O.getActiveCubeFace(),Te=O.getActiveMipmapLevel();O.setRenderTarget(me),O.getClearColor(be),we=O.getClearAlpha(),we<1&&O.setClearColor(16777215,.5),O.clear(),Ct&&re.render(G);let He=O.toneMapping;O.toneMapping=mi;let je=V.viewport;if(V.viewport!==void 0&&(V.viewport=void 0),T.setupLightsView(V),wt===!0&&xe.setGlobalState(O.clippingPlanes,V),Qa(_,G,V),v.updateMultisampleRenderTarget(me),v.updateRenderTargetMipmap(me),Ze.has("WEBGL_multisampled_render_to_texture")===!1){let Ce=!1;for(let pt=0,Ut=N.length;pt<Ut;pt++){let Rt=N[pt],{object:_t,geometry:tn,material:_e,group:Cn}=Rt;if(_e.side===Ni&&_t.layers.test(V.layers)){let et=_e.side;_e.side=xn,_e.needsUpdate=!0,hg(_t,G,V,tn,_e,Cn),_e.side=et,_e.needsUpdate=!0,Ce=!0}}Ce===!0&&(v.updateMultisampleRenderTarget(me),v.updateRenderTargetMipmap(me))}O.setRenderTarget(pe,Me,Te),O.setClearColor(be,we),je!==void 0&&(V.viewport=je),O.toneMapping=He}function Qa(_,N,G){let V=N.isScene===!0?N.overrideMaterial:null;for(let H=0,me=_.length;H<me;H++){let Ee=_[H],{object:pe,geometry:Me,group:Te}=Ee,He=Ee.material;He.allowOverride===!0&&V!==null&&(He=V),pe.layers.test(G.layers)&&hg(pe,N,G,Me,He,Te)}}function hg(_,N,G,V,H,me){_.onBeforeRender(O,N,G,V,H,me),_.modelViewMatrix.multiplyMatrices(G.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),H.onBeforeRender(O,N,G,V,_,me),H.transparent===!0&&H.side===Ni&&H.forceSinglePass===!1?(H.side=xn,H.needsUpdate=!0,O.renderBufferDirect(G,N,V,H,_,me),H.side=Qi,H.needsUpdate=!0,O.renderBufferDirect(G,N,V,H,_,me),H.side=Ni):O.renderBufferDirect(G,N,V,H,_,me),_.onAfterRender(O,N,G,V,H,me)}function ec(_,N,G){N.isScene!==!0&&(N=Wt);let V=E.get(_),H=T.state.lights,me=T.state.shadowsArray,Ee=H.state.version,pe=ue.getParameters(_,H.state,me,N,G,T.state.lightProbeGridArray),Me=ue.getProgramCacheKey(pe),Te=V.programs;V.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?N.environment:null,V.fog=N.fog;let He=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;V.envMap=F.get(_.envMap||V.environment,He),V.envMapRotation=V.environment!==null&&_.envMap===null?N.environmentRotation:_.envMapRotation,Te===void 0&&(_.addEventListener("dispose",kt),Te=new Map,V.programs=Te);let je=Te.get(Me);if(je!==void 0){if(V.currentProgram===je&&V.lightsStateVersion===Ee)return mg(_,pe),je}else pe.uniforms=ue.getUniforms(_),L!==null&&_.isNodeMaterial&&L.build(_,G,pe),_.onBeforeCompile(pe,O),je=ue.acquireProgram(pe,Me),Te.set(Me,je),V.uniforms=pe.uniforms;let Ce=V.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Ce.clippingPlanes=xe.uniform),mg(_,pe),V.needsLights=dE(_),V.lightsStateVersion=Ee,V.needsLights&&(Ce.ambientLightColor.value=H.state.ambient,Ce.lightProbe.value=H.state.probe,Ce.directionalLights.value=H.state.directional,Ce.directionalLightShadows.value=H.state.directionalShadow,Ce.spotLights.value=H.state.spot,Ce.spotLightShadows.value=H.state.spotShadow,Ce.rectAreaLights.value=H.state.rectArea,Ce.ltc_1.value=H.state.rectAreaLTC1,Ce.ltc_2.value=H.state.rectAreaLTC2,Ce.pointLights.value=H.state.point,Ce.pointLightShadows.value=H.state.pointShadow,Ce.hemisphereLights.value=H.state.hemi,Ce.directionalShadowMatrix.value=H.state.directionalShadowMatrix,Ce.spotLightMatrix.value=H.state.spotLightMatrix,Ce.spotLightMap.value=H.state.spotLightMap,Ce.pointShadowMatrix.value=H.state.pointShadowMatrix),V.lightProbeGrid=T.state.lightProbeGridArray.length>0,V.currentProgram=je,V.uniformsList=null,je}function pg(_){if(_.uniformsList===null){let N=_.currentProgram.getUniforms();_.uniformsList=Eo.seqWithValue(N.seq,_.uniforms)}return _.uniformsList}function mg(_,N){let G=E.get(_);G.outputColorSpace=N.outputColorSpace,G.batching=N.batching,G.batchingColor=N.batchingColor,G.instancing=N.instancing,G.instancingColor=N.instancingColor,G.instancingMorph=N.instancingMorph,G.skinning=N.skinning,G.morphTargets=N.morphTargets,G.morphNormals=N.morphNormals,G.morphColors=N.morphColors,G.morphTargetsCount=N.morphTargetsCount,G.numClippingPlanes=N.numClippingPlanes,G.numIntersection=N.numClipIntersection,G.vertexAlphas=N.vertexAlphas,G.vertexTangents=N.vertexTangents,G.toneMapping=N.toneMapping}function cE(_,N){if(_.length===0)return null;if(_.length===1)return _[0].texture!==null?_[0]:null;S.setFromMatrixPosition(N.matrixWorld);for(let G=0,V=_.length;G<V;G++){let H=_[G];if(H.texture!==null&&H.boundingBox.containsPoint(S))return H}return null}function lE(_,N,G,V,H){N.isScene!==!0&&(N=Wt),v.resetTextureUnits();let me=N.fog,Ee=V.isMeshStandardMaterial||V.isMeshLambertMaterial||V.isMeshPhongMaterial?N.environment:null,pe=P===null?O.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:Je.workingColorSpace,Me=V.isMeshStandardMaterial||V.isMeshLambertMaterial&&!V.envMap||V.isMeshPhongMaterial&&!V.envMap,Te=F.get(V.envMap||Ee,Me),He=V.vertexColors===!0&&!!G.attributes.color&&G.attributes.color.itemSize===4,je=!!G.attributes.tangent&&(!!V.normalMap||V.anisotropy>0),Ce=!!G.morphAttributes.position,pt=!!G.morphAttributes.normal,Ut=!!G.morphAttributes.color,Rt=mi;V.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(Rt=O.toneMapping);let _t=G.morphAttributes.position||G.morphAttributes.normal||G.morphAttributes.color,tn=_t!==void 0?_t.length:0,_e=E.get(V),Cn=T.state.lights;if(wt===!0&&(Ye===!0||_!==k)){let bt=_===k&&V.id===W;xe.setState(V,_,bt)}let et=!1;V.version===_e.__version?(_e.needsLights&&_e.lightsStateVersion!==Cn.state.version||_e.outputColorSpace!==pe||H.isBatchedMesh&&_e.batching===!1||!H.isBatchedMesh&&_e.batching===!0||H.isBatchedMesh&&_e.batchingColor===!0&&H.colorTexture===null||H.isBatchedMesh&&_e.batchingColor===!1&&H.colorTexture!==null||H.isInstancedMesh&&_e.instancing===!1||!H.isInstancedMesh&&_e.instancing===!0||H.isSkinnedMesh&&_e.skinning===!1||!H.isSkinnedMesh&&_e.skinning===!0||H.isInstancedMesh&&_e.instancingColor===!0&&H.instanceColor===null||H.isInstancedMesh&&_e.instancingColor===!1&&H.instanceColor!==null||H.isInstancedMesh&&_e.instancingMorph===!0&&H.morphTexture===null||H.isInstancedMesh&&_e.instancingMorph===!1&&H.morphTexture!==null||_e.envMap!==Te||V.fog===!0&&_e.fog!==me||_e.numClippingPlanes!==void 0&&(_e.numClippingPlanes!==xe.numPlanes||_e.numIntersection!==xe.numIntersection)||_e.vertexAlphas!==He||_e.vertexTangents!==je||_e.morphTargets!==Ce||_e.morphNormals!==pt||_e.morphColors!==Ut||_e.toneMapping!==Rt||_e.morphTargetsCount!==tn||!!_e.lightProbeGrid!=T.state.lightProbeGridArray.length>0)&&(et=!0):(et=!0,_e.__version=V.version);let Vn=_e.currentProgram;et===!0&&(Vn=ec(V,N,H),L&&V.isNodeMaterial&&L.onUpdateProgram(V,Vn,_e));let _i=!1,sr=!1,Es=!1,xt=Vn.getUniforms(),Bt=_e.uniforms;if(de.useProgram(Vn.program)&&(_i=!0,sr=!0,Es=!0),V.id!==W&&(W=V.id,sr=!0),_e.needsLights){let bt=cE(T.state.lightProbeGridArray,H);_e.lightProbeGrid!==bt&&(_e.lightProbeGrid=bt,sr=!0)}if(_i||k!==_){de.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),xt.setValue(R,"projectionMatrix",_.projectionMatrix),xt.setValue(R,"viewMatrix",_.matrixWorldInverse);let ar=xt.map.cameraPosition;ar!==void 0&&ar.setValue(R,Mt.setFromMatrixPosition(_.matrixWorld)),Et.logarithmicDepthBuffer&&xt.setValue(R,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(V.isMeshPhongMaterial||V.isMeshToonMaterial||V.isMeshLambertMaterial||V.isMeshBasicMaterial||V.isMeshStandardMaterial||V.isShaderMaterial)&&xt.setValue(R,"isOrthographic",_.isOrthographicCamera===!0),k!==_&&(k=_,sr=!0,Es=!0)}if(_e.needsLights&&(Cn.state.directionalShadowMap.length>0&&xt.setValue(R,"directionalShadowMap",Cn.state.directionalShadowMap,v),Cn.state.spotShadowMap.length>0&&xt.setValue(R,"spotShadowMap",Cn.state.spotShadowMap,v),Cn.state.pointShadowMap.length>0&&xt.setValue(R,"pointShadowMap",Cn.state.pointShadowMap,v)),H.isSkinnedMesh){xt.setOptional(R,H,"bindMatrix"),xt.setOptional(R,H,"bindMatrixInverse");let bt=H.skeleton;bt&&(bt.boneTexture===null&&bt.computeBoneTexture(),xt.setValue(R,"boneTexture",bt.boneTexture,v))}H.isBatchedMesh&&(xt.setOptional(R,H,"batchingTexture"),xt.setValue(R,"batchingTexture",H._matricesTexture,v),xt.setOptional(R,H,"batchingIdTexture"),xt.setValue(R,"batchingIdTexture",H._indirectTexture,v),xt.setOptional(R,H,"batchingColorTexture"),H._colorsTexture!==null&&xt.setValue(R,"batchingColorTexture",H._colorsTexture,v));let or=G.morphAttributes;if((or.position!==void 0||or.normal!==void 0||or.color!==void 0)&&Oe.update(H,G,Vn),(sr||_e.receiveShadow!==H.receiveShadow)&&(_e.receiveShadow=H.receiveShadow,xt.setValue(R,"receiveShadow",H.receiveShadow)),(V.isMeshStandardMaterial||V.isMeshLambertMaterial||V.isMeshPhongMaterial)&&V.envMap===null&&N.environment!==null&&(Bt.envMapIntensity.value=N.environmentIntensity),Bt.dfgLUT!==void 0&&(Bt.dfgLUT.value=fR()),sr){if(xt.setValue(R,"toneMappingExposure",O.toneMappingExposure),_e.needsLights&&uE(Bt,Es),me&&V.fog===!0&&q.refreshFogUniforms(Bt,me),q.refreshMaterialUniforms(Bt,V,ze,dt,T.state.transmissionRenderTarget[_.id]),_e.needsLights&&_e.lightProbeGrid){let bt=_e.lightProbeGrid;Bt.probesSH.value=bt.texture,Bt.probesMin.value.copy(bt.boundingBox.min),Bt.probesMax.value.copy(bt.boundingBox.max),Bt.probesResolution.value.copy(bt.resolution)}Eo.upload(R,pg(_e),Bt,v)}if(V.isShaderMaterial&&V.uniformsNeedUpdate===!0&&(Eo.upload(R,pg(_e),Bt,v),V.uniformsNeedUpdate=!1),V.isSpriteMaterial&&xt.setValue(R,"center",H.center),xt.setValue(R,"modelViewMatrix",H.modelViewMatrix),xt.setValue(R,"normalMatrix",H.normalMatrix),xt.setValue(R,"modelMatrix",H.matrixWorld),V.uniformsGroups!==void 0){let bt=V.uniformsGroups;for(let ar=0,bs=bt.length;ar<bs;ar++){let gg=bt[ar];Y.update(gg,Vn),Y.bind(gg,Vn)}}return Vn}function uE(_,N){_.ambientLightColor.needsUpdate=N,_.lightProbe.needsUpdate=N,_.directionalLights.needsUpdate=N,_.directionalLightShadows.needsUpdate=N,_.pointLights.needsUpdate=N,_.pointLightShadows.needsUpdate=N,_.spotLights.needsUpdate=N,_.spotLightShadows.needsUpdate=N,_.rectAreaLights.needsUpdate=N,_.hemisphereLights.needsUpdate=N}function dE(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return $},this.getActiveMipmapLevel=function(){return X},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(_,N,G){let V=E.get(_);V.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,V.__autoAllocateDepthBuffer===!1&&(V.__useRenderToTexture=!1),E.get(_.texture).__webglTexture=N,E.get(_.depthTexture).__webglTexture=V.__autoAllocateDepthBuffer?void 0:G,V.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,N){let G=E.get(_);G.__webglFramebuffer=N,G.__useDefaultFramebuffer=N===void 0};let fE=R.createFramebuffer();this.setRenderTarget=function(_,N=0,G=0){P=_,$=N,X=G;let V=null,H=!1,me=!1;if(_){let pe=E.get(_);if(pe.__useDefaultFramebuffer!==void 0){de.bindFramebuffer(R.FRAMEBUFFER,pe.__webglFramebuffer),te.copy(_.viewport),ne.copy(_.scissor),he=_.scissorTest,de.viewport(te),de.scissor(ne),de.setScissorTest(he),W=-1;return}else if(pe.__webglFramebuffer===void 0)v.setupRenderTarget(_);else if(pe.__hasExternalTextures)v.rebindTextures(_,E.get(_.texture).__webglTexture,E.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let He=_.depthTexture;if(pe.__boundDepthTexture!==He){if(He!==null&&E.has(He)&&(_.width!==He.image.width||_.height!==He.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");v.setupDepthRenderbuffer(_)}}let Me=_.texture;(Me.isData3DTexture||Me.isDataArrayTexture||Me.isCompressedArrayTexture)&&(me=!0);let Te=E.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Te[N])?V=Te[N][G]:V=Te[N],H=!0):_.samples>0&&v.useMultisampledRTT(_)===!1?V=E.get(_).__webglMultisampledFramebuffer:Array.isArray(Te)?V=Te[G]:V=Te,te.copy(_.viewport),ne.copy(_.scissor),he=_.scissorTest}else te.copy(ae).multiplyScalar(ze).floor(),ne.copy(Re).multiplyScalar(ze).floor(),he=Be;if(G!==0&&(V=fE),de.bindFramebuffer(R.FRAMEBUFFER,V)&&de.drawBuffers(_,V),de.viewport(te),de.scissor(ne),de.setScissorTest(he),H){let pe=E.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_CUBE_MAP_POSITIVE_X+N,pe.__webglTexture,G)}else if(me){let pe=N;for(let Me=0;Me<_.textures.length;Me++){let Te=E.get(_.textures[Me]);R.framebufferTextureLayer(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0+Me,Te.__webglTexture,G,pe)}}else if(_!==null&&G!==0){let pe=E.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,pe.__webglTexture,G)}W=-1},this.readRenderTargetPixels=function(_,N,G,V,H,me,Ee,pe=0){if(!(_&&_.isWebGLRenderTarget)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Me=E.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&Ee!==void 0&&(Me=Me[Ee]),Me){de.bindFramebuffer(R.FRAMEBUFFER,Me);try{let Te=_.textures[pe],He=Te.format,je=Te.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+pe),!Et.textureFormatReadable(He)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Et.textureTypeReadable(je)){Ne("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=_.width-V&&G>=0&&G<=_.height-H&&R.readPixels(N,G,V,H,A.convert(He),A.convert(je),me)}finally{let Te=P!==null?E.get(P).__webglFramebuffer:null;de.bindFramebuffer(R.FRAMEBUFFER,Te)}}},this.readRenderTargetPixelsAsync=async function(_,N,G,V,H,me,Ee,pe=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Me=E.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&Ee!==void 0&&(Me=Me[Ee]),Me)if(N>=0&&N<=_.width-V&&G>=0&&G<=_.height-H){de.bindFramebuffer(R.FRAMEBUFFER,Me);let Te=_.textures[pe],He=Te.format,je=Te.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+pe),!Et.textureFormatReadable(He))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Et.textureTypeReadable(je))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ce=R.createBuffer();R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.bufferData(R.PIXEL_PACK_BUFFER,me.byteLength,R.STREAM_READ),R.readPixels(N,G,V,H,A.convert(He),A.convert(je),0);let pt=P!==null?E.get(P).__webglFramebuffer:null;de.bindFramebuffer(R.FRAMEBUFFER,pt);let Ut=R.fenceSync(R.SYNC_GPU_COMMANDS_COMPLETE,0);return R.flush(),await wx(R,Ut,4),R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.getBufferSubData(R.PIXEL_PACK_BUFFER,0,me),R.deleteBuffer(Ce),R.deleteSync(Ut),me}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,N=null,G=0){let V=Math.pow(2,-G),H=Math.floor(_.image.width*V),me=Math.floor(_.image.height*V),Ee=N!==null?N.x:0,pe=N!==null?N.y:0;v.setTexture2D(_,0),R.copyTexSubImage2D(R.TEXTURE_2D,G,0,0,Ee,pe,H,me),de.unbindTexture()};let hE=R.createFramebuffer(),pE=R.createFramebuffer();this.copyTextureToTexture=function(_,N,G=null,V=null,H=0,me=0){let Ee,pe,Me,Te,He,je,Ce,pt,Ut,Rt=_.isCompressedTexture?_.mipmaps[me]:_.image;if(G!==null)Ee=G.max.x-G.min.x,pe=G.max.y-G.min.y,Me=G.isBox3?G.max.z-G.min.z:1,Te=G.min.x,He=G.min.y,je=G.isBox3?G.min.z:0;else{let Bt=Math.pow(2,-H);Ee=Math.floor(Rt.width*Bt),pe=Math.floor(Rt.height*Bt),_.isDataArrayTexture?Me=Rt.depth:_.isData3DTexture?Me=Math.floor(Rt.depth*Bt):Me=1,Te=0,He=0,je=0}V!==null?(Ce=V.x,pt=V.y,Ut=V.z):(Ce=0,pt=0,Ut=0);let _t=A.convert(N.format),tn=A.convert(N.type),_e;N.isData3DTexture?(v.setTexture3D(N,0),_e=R.TEXTURE_3D):N.isDataArrayTexture||N.isCompressedArrayTexture?(v.setTexture2DArray(N,0),_e=R.TEXTURE_2D_ARRAY):(v.setTexture2D(N,0),_e=R.TEXTURE_2D),de.activeTexture(R.TEXTURE0),de.pixelStorei(R.UNPACK_FLIP_Y_WEBGL,N.flipY),de.pixelStorei(R.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),de.pixelStorei(R.UNPACK_ALIGNMENT,N.unpackAlignment);let Cn=de.getParameter(R.UNPACK_ROW_LENGTH),et=de.getParameter(R.UNPACK_IMAGE_HEIGHT),Vn=de.getParameter(R.UNPACK_SKIP_PIXELS),_i=de.getParameter(R.UNPACK_SKIP_ROWS),sr=de.getParameter(R.UNPACK_SKIP_IMAGES);de.pixelStorei(R.UNPACK_ROW_LENGTH,Rt.width),de.pixelStorei(R.UNPACK_IMAGE_HEIGHT,Rt.height),de.pixelStorei(R.UNPACK_SKIP_PIXELS,Te),de.pixelStorei(R.UNPACK_SKIP_ROWS,He),de.pixelStorei(R.UNPACK_SKIP_IMAGES,je);let Es=_.isDataArrayTexture||_.isData3DTexture,xt=N.isDataArrayTexture||N.isData3DTexture;if(_.isDepthTexture){let Bt=E.get(_),or=E.get(N),bt=E.get(Bt.__renderTarget),ar=E.get(or.__renderTarget);de.bindFramebuffer(R.READ_FRAMEBUFFER,bt.__webglFramebuffer),de.bindFramebuffer(R.DRAW_FRAMEBUFFER,ar.__webglFramebuffer);for(let bs=0;bs<Me;bs++)Es&&(R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,E.get(_).__webglTexture,H,je+bs),R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,E.get(N).__webglTexture,me,Ut+bs)),R.blitFramebuffer(Te,He,Ee,pe,Ce,pt,Ee,pe,R.DEPTH_BUFFER_BIT,R.NEAREST);de.bindFramebuffer(R.READ_FRAMEBUFFER,null),de.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else if(H!==0||_.isRenderTargetTexture||E.has(_)){let Bt=E.get(_),or=E.get(N);de.bindFramebuffer(R.READ_FRAMEBUFFER,hE),de.bindFramebuffer(R.DRAW_FRAMEBUFFER,pE);for(let bt=0;bt<Me;bt++)Es?R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,Bt.__webglTexture,H,je+bt):R.framebufferTexture2D(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,Bt.__webglTexture,H),xt?R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,or.__webglTexture,me,Ut+bt):R.framebufferTexture2D(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,or.__webglTexture,me),H!==0?R.blitFramebuffer(Te,He,Ee,pe,Ce,pt,Ee,pe,R.COLOR_BUFFER_BIT,R.NEAREST):xt?R.copyTexSubImage3D(_e,me,Ce,pt,Ut+bt,Te,He,Ee,pe):R.copyTexSubImage2D(_e,me,Ce,pt,Te,He,Ee,pe);de.bindFramebuffer(R.READ_FRAMEBUFFER,null),de.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else xt?_.isDataTexture||_.isData3DTexture?R.texSubImage3D(_e,me,Ce,pt,Ut,Ee,pe,Me,_t,tn,Rt.data):N.isCompressedArrayTexture?R.compressedTexSubImage3D(_e,me,Ce,pt,Ut,Ee,pe,Me,_t,Rt.data):R.texSubImage3D(_e,me,Ce,pt,Ut,Ee,pe,Me,_t,tn,Rt):_.isDataTexture?R.texSubImage2D(R.TEXTURE_2D,me,Ce,pt,Ee,pe,_t,tn,Rt.data):_.isCompressedTexture?R.compressedTexSubImage2D(R.TEXTURE_2D,me,Ce,pt,Rt.width,Rt.height,_t,Rt.data):R.texSubImage2D(R.TEXTURE_2D,me,Ce,pt,Ee,pe,_t,tn,Rt);de.pixelStorei(R.UNPACK_ROW_LENGTH,Cn),de.pixelStorei(R.UNPACK_IMAGE_HEIGHT,et),de.pixelStorei(R.UNPACK_SKIP_PIXELS,Vn),de.pixelStorei(R.UNPACK_SKIP_ROWS,_i),de.pixelStorei(R.UNPACK_SKIP_IMAGES,sr),me===0&&N.generateMipmaps&&R.generateMipmap(_e),de.unbindTexture()},this.initRenderTarget=function(_){E.get(_).__webglFramebuffer===void 0&&v.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?v.setTextureCube(_,0):_.isData3DTexture?v.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?v.setTexture2DArray(_,0):v.setTexture2D(_,0),de.unbindTexture()},this.resetState=function(){$=0,X=0,P=null,de.reset(),se.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return pi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=Je._getDrawingBufferColorSpace(e),t.unpackColorSpace=Je._getUnpackColorSpace()}};var pR=["heroCanvas"],mR=["titleRef"],gR=["subtitleRef"],vR=["btnGroupRef"];function yR(n,e){let t=e.getBoundingClientRect(),i=document.createElement("span");i.className="ripple";let r=Math.max(t.width,t.height);i.style.cssText=`width:${r}px;height:${r}px;left:${n.clientX-t.left-r/2}px;top:${n.clientY-t.top-r/2}px;`,e.appendChild(i),setTimeout(()=>i.remove(),700)}function _R(n,e){let t=e.getBoundingClientRect(),i=(n.clientX-t.left)/t.width*100,r=(n.clientY-t.top)/t.height*100;e.style.setProperty("--x",`${i}%`),e.style.setProperty("--y",`${r}%`)}var Bd=class n{canvasRef=yn("heroCanvas");titleRef=yn("titleRef");subtitleRef=yn("subtitleRef");btnGroupRef=yn("btnGroupRef");destroyRef=Ie(It);ngZone=Ie(mn);addLightFollow(e){let t=e.currentTarget;t&&_R(e,t)}onClickRipple(e,t){yR(e,e.currentTarget),document.querySelector(t)?.scrollIntoView({behavior:"smooth"})}constructor(){Qt(()=>{this.ngZone.runOutsideAngular(()=>{let i=this.canvasRef()?.nativeElement;if(!i)return;let r=new Ca,s=new dn(75,window.innerWidth/window.innerHeight,.1,1e3);s.position.z=5;let o=new Fd({canvas:i,alpha:!0,antialias:!0});o.setSize(window.innerWidth,window.innerHeight),o.setPixelRatio(Math.min(window.devicePixelRatio,2));let a=180,c=new Float32Array(a*3),l=new Float32Array(a*3);for(let I=0;I<a*3;I++)c[I]=(Math.random()-.5)*18,l[I]=(Math.random()-.5)*.0025;let u=new _n;u.setAttribute("position",new fn(c,3));let f=new mo({color:2989275,size:.055,transparent:!0,opacity:.75,sizeAttenuation:!0}),d=new Pa(u,f);r.add(d);let h=250,g=new Float32Array(h*6),x=new _n;x.setAttribute("position",new fn(g,3));let m=new po({color:2989275,transparent:!0,opacity:.1}),p=new Na(x,m);r.add(p);let b={x:0,y:0},M=I=>{b.x=I.clientX/window.innerWidth*2-1,b.y=-(I.clientY/window.innerHeight)*2+1};window.addEventListener("mousemove",M);let S=0,D=()=>{S=requestAnimationFrame(D);let I=d.geometry.attributes.position.array;for(let C=0;C<a;C++){let L=C*3,$=C*3+1,X=C*3+2;I[L]+=l[L],I[$]+=l[$],I[X]+=l[X],Math.abs(I[L])>9&&(l[L]*=-1),Math.abs(I[$])>9&&(l[$]*=-1),Math.abs(I[X])>9&&(l[X]*=-1)}d.geometry.attributes.position.needsUpdate=!0;let y=0,w=x.attributes.position.array,O=4;for(let C=0;C<a&&y<h;C++)for(let L=C+1;L<a&&y<h;L++){let $=I[C*3]-I[L*3],X=I[C*3+1]-I[L*3+1],P=I[C*3+2]-I[L*3+2];if(Math.sqrt($*$+X*X+P*P)<O){let k=y*6;w[k]=I[C*3],w[k+1]=I[C*3+1],w[k+2]=I[C*3+2],w[k+3]=I[L*3],w[k+4]=I[L*3+1],w[k+5]=I[L*3+2],y++}}x.setDrawRange(0,y*2),x.attributes.position.needsUpdate=!0,d.rotation.y+=6e-4,d.rotation.x+=2e-4,s.position.x+=(b.x*.7-s.position.x)*.05,s.position.y+=(b.y*.5-s.position.y)*.05,o.render(r,s)};D();let T=()=>{s.aspect=window.innerWidth/window.innerHeight,s.updateProjectionMatrix(),o.setSize(window.innerWidth,window.innerHeight)};window.addEventListener("resize",T),this.destroyRef.onDestroy(()=>{cancelAnimationFrame(S),window.removeEventListener("mousemove",M),window.removeEventListener("resize",T),o.dispose(),u.dispose(),f.dispose()})});let e=i=>new Promise(r=>setTimeout(r,i));(async()=>{let i=this.titleRef()?.nativeElement,r=this.subtitleRef()?.nativeElement,s=this.btnGroupRef()?.nativeElement;if(!i||!r||!s)return;let o=i.textContent||"";i.innerHTML="",o.split("").forEach(c=>{let l=document.createElement("span");l.className="hero-char";let u=document.createElement("span");u.className="hero-char-inner",u.textContent=c===" "?"\xA0":c,l.appendChild(u),i.appendChild(l)}),await e(300);let a=i.querySelectorAll(".hero-char-inner");a.forEach((c,l)=>{setTimeout(()=>{c.style.transform="translateY(0)"},l*35)}),await e(a.length*35+400),r.style.opacity="1",r.style.transform="translateY(0)",await e(300),s.style.opacity="1",s.style.transform="translateY(0)"})()})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-hero-section"]],viewQuery:function(t,i){t&1&&Zn(i.canvasRef,pR,5)(i.titleRef,mR,5)(i.subtitleRef,gR,5)(i.btnGroupRef,vR,5),t&2&&ui(4)},decls:29,vars:0,consts:[["heroCanvas",""],["titleRef",""],["subtitleRef",""],["btnGroupRef",""],["id","hero",1,"hero-section"],["id","heroCanvas"],[1,"pill","pill-1"],[1,"pill","pill-2"],[1,"pill","pill-3"],[1,"pill","pill-4"],[1,"pill","pill-5"],[1,"pill","pill-6"],[1,"hero-content"],[2,"margin-bottom","1.5rem"],[1,"badge-label"],[2,"color","#2D9CDB"],[2,"font-size","clamp(2rem, 5vw, 3.8rem)","font-weight","900","line-height","1.3","color","#ffffff","margin-bottom","1.5rem","letter-spacing","0.02em"],[2,"font-size","clamp(1rem, 2.5vw, 1.25rem)","color","rgba(184,228,249,0.85)","margin-bottom","2.5rem","font-weight","400","line-height","1.8","max-width","650px","margin-left","auto","margin-right","auto","opacity","0","transform","translateY(30px)","transition","opacity 0.9s ease, transform 0.9s ease"],[2,"display","flex","gap","1.2rem","justify-content","center","flex-wrap","wrap","opacity","0","transform","translateY(20px)","transition","opacity 0.7s ease, transform 0.7s ease"],[1,"btn-primary",2,"font-size","1.05rem","padding","0.95rem 2.4rem",3,"click","mousemove"],[1,"btn-secondary",2,"font-size","1.05rem","padding","0.95rem 2.4rem",3,"click","mousemove"],[2,"margin-top","4rem","animation","bounceArrow 2s ease-in-out infinite","color","rgba(184,228,249,0.5)","font-size","1.5rem"]],template:function(t,i){t&1&&(U(0,"section",4),an(1,"canvas",5,0)(3,"div",6)(4,"div",7)(5,"div",8)(6,"div",9)(7,"div",10)(8,"div",11),U(9,"div",12)(10,"div",13)(11,"span",14)(12,"span",15),j(13,"\u25CF"),z(),j(14," \u062B\u0642\u062A\u0643 \u0641\u064A \u0635\u062D\u062A\u0643 \u0645\u0646\u0630 \u0623\u0643\u062B\u0631 \u0645\u0646 20 \u0639\u0627\u0645\u0627\u064B "),z()(),U(15,"h1",16,1),j(17," \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u2014 \u062B\u0642\u062A\u0643 \u0641\u064A \u0635\u062D\u062A\u0643 "),z(),U(18,"p",17,2),j(20," \u0646\u0648\u0641\u0631 \u0623\u0641\u0636\u0644 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629 \u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u062C\u0648\u062F\u0629 \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629\u060C \u0645\u0639 \u0627\u0644\u062A\u0632\u0627\u0645\u0646\u0627 \u0627\u0644\u0643\u0627\u0645\u0644 \u0628\u0635\u062D\u062A\u0643 \u0648\u0633\u0644\u0627\u0645\u0629 \u0639\u0627\u0626\u0644\u062A\u0643 "),z(),U(21,"div",18,3)(23,"button",19),lt("click",function(s){return i.onClickRipple(s,"#products")})("mousemove",function(s){return i.addLightFollow(s)}),j(24,"\u0627\u0643\u062A\u0634\u0641 \u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"),z(),U(25,"button",20),lt("click",function(s){return i.onClickRipple(s,"#contact")})("mousemove",function(s){return i.addLightFollow(s)}),j(26,"\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0646\u0627"),z()(),U(27,"div",21),j(28,"\u2193"),z()()())},styles:["@keyframes _ngcontent-%COMP%_bounceArrow{0%,to{transform:translateY(0);opacity:.5}50%{transform:translateY(10px);opacity:1}}"]})};var xR=(n,e)=>e.label;function ER(n,e){n&1&&an(0,"div",7)}function bR(n,e){if(n&1&&(U(0,"div",6),ci(1,ER,1,0,"div",7),U(2,"div",8),j(3),z(),U(4,"div",9,0),j(6),z(),U(7,"div",10),j(8),z()()),n&2){let t=e.$implicit,i=e.$index,r=Tt();At("transition-delay",i*.15+"s"),oe(),li(i<r.stats.length-1?1:-1),oe(2),ke(t.icon),oe(3),Nn("0",t.suffix),oe(2),ke(t.label)}}var ag=[{number:500,label:"\u0645\u0646\u062A\u062C \u062F\u0648\u0627\u0626\u064A",icon:"\u{1F48A}",suffix:"+"},{number:20,label:"\u0633\u0646\u0629 \u062E\u0628\u0631\u0629",icon:"\u{1F4C5}",suffix:"+"},{number:1e3,label:"\u0639\u0645\u064A\u0644 \u0631\u0627\u0636\u064D",icon:"\u{1F60A}",suffix:"+"},{number:50,label:"\u0634\u0631\u064A\u0643 \u062F\u0648\u0644\u064A",icon:"\u{1F30D}",suffix:"+"}],Vd=class n{stats=ag;constructor(){Qt(()=>{document.querySelectorAll(".stats-section .reveal-up").forEach(t=>{let i=new IntersectionObserver(([r])=>{r.isIntersecting&&(t.classList.add("visible"),i.disconnect())},{threshold:.2});i.observe(t)}),document.querySelectorAll(".stats-section .stat-number").forEach((t,i)=>{if(i>=ag.length)return;let r=ag[i],s=new IntersectionObserver(([o])=>{if(o.isIntersecting){let a=r.number,c=r.suffix,l=2200,u=performance.now(),f=d=>{let h=Math.min((d-u)/l,1),g=1-Math.pow(1-h,3);t.textContent=Math.round(g*a)+c,h<1&&requestAnimationFrame(f)};requestAnimationFrame(f),s.disconnect()}},{threshold:.5});s.observe(t)})})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-stats-section"]],decls:6,vars:0,consts:[["counterEl",""],[1,"stats-section",2,"position","relative","overflow","hidden"],[2,"position","absolute","top","50%","left","50%","transform","translate(-50%,-50%)","width","600px","height","600px","background","radial-gradient(circle, rgba(45,156,219,0.08) 0%, transparent 70%)","pointer-events","none"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(200px, 1fr))","gap","0"],[1,"reveal-up",2,"text-align","center","padding","2.5rem 2rem","position","relative",3,"transition-delay"],[1,"reveal-up",2,"text-align","center","padding","2.5rem 2rem","position","relative"],[2,"position","absolute","top","20%","left","0","width","1px","height","60%","background","rgba(184,228,249,0.15)"],[2,"font-size","2.5rem","margin-bottom","1rem","filter","drop-shadow(0 0 15px rgba(45,156,219,0.6))"],[1,"stat-number",2,"margin-bottom","0.5rem"],[2,"color","rgba(184,228,249,0.7)","font-size","1rem","font-weight","500"]],template:function(t,i){t&1&&(U(0,"section",1),an(1,"div",2),U(2,"div",3)(3,"div",4),vt(4,bR,9,6,"div",5,xR),z()()()),t&2&&(oe(4),yt(i.stats))},encapsulation:2})};var Hd=class n{fromColor=Dl("#F0FAFF");toColor=Dl("#071525");flip=Dl(!1);static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-wave-separator"]],inputs:{fromColor:[1,"fromColor"],toColor:[1,"toColor"],flip:[1,"flip"]},decls:4,vars:8,consts:[[1,"wave-separator"],["viewBox","0 0 1440 70","preserveAspectRatio","none",2,"width","100%","height","70px","display","block"],["opacity","0.5"]],template:function(t,i){t&1&&(U(0,"div",0),Vc(),U(1,"svg",1),an(2,"path",2)(3,"path"),z()()),t&2&&(At("background",i.fromColor())("transform",i.flip()?"scaleY(-1)":"none"),oe(2),ia("d","M0,35 C240,70 480,0 720,35 C960,70 1200,10 1440,35 L1440,70 L0,70 Z")("fill",i.toColor()),oe(),ia("d","M0,50 C360,15 720,65 1080,40 C1260,28 1380,38 1440,50 L1440,70 L0,70 Z")("fill",i.toColor()))},encapsulation:2})};var MR=(n,e)=>e.title;function SR(n,e){if(n&1&&(U(0,"div",27)(1,"div",28)(2,"span",29),j(3),z()(),U(4,"div")(5,"div",30),j(6),z(),U(7,"div",31),j(8),z()()()),n&2){let t=e.$implicit,i=e.$index;At("transition-delay",i*.12+"s"),oe(3),ke(t.icon),oe(3),ke(t.title),oe(2),ke(t.desc)}}var TR=[{icon:"\u2705",title:"\u062C\u0648\u062F\u0629 \u0645\u0639\u062A\u0645\u062F\u0629 \u062F\u0648\u0644\u064A\u0627\u064B",desc:"\u0646\u0644\u062A\u0632\u0645 \u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u062C\u0648\u062F\u0629 \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629 \u0641\u064A \u062C\u0645\u064A\u0639 \u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"},{icon:"\u{1F69A}",title:"\u062A\u0648\u0632\u064A\u0639 \u0633\u0631\u064A\u0639 \u0648\u0645\u0648\u062B\u0648\u0642",desc:"\u0634\u0628\u0643\u0629 \u062A\u0648\u0632\u064A\u0639 \u0648\u0627\u0633\u0639\u0629 \u062A\u063A\u0637\u064A \u062C\u0645\u064A\u0639 \u0623\u0646\u062D\u0627\u0621 \u0627\u0644\u0628\u0644\u0627\u062F"},{icon:"\u{1F52C}",title:"\u0623\u0628\u062D\u0627\u062B \u0645\u062A\u0637\u0648\u0631\u0629",desc:"\u0646\u0633\u062A\u062B\u0645\u0631 \u0641\u064A \u0627\u0644\u0628\u062D\u062B \u0648\u0627\u0644\u062A\u0637\u0648\u064A\u0631 \u0644\u062A\u0648\u0641\u064A\u0631 \u0623\u062D\u062F\u062B \u0627\u0644\u0623\u062F\u0648\u064A\u0629"},{icon:"\u{1F91D}",title:"\u0634\u0631\u0627\u0643\u0627\u062A \u0627\u0633\u062A\u0631\u0627\u062A\u064A\u062C\u064A\u0629",desc:"\u062A\u0639\u0627\u0648\u0646 \u0645\u0639 \u0623\u0643\u0628\u0631 \u0634\u0631\u0643\u0627\u062A \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629"}],zd=class n{features=TR;constructor(){Qt(()=>{let e=new IntersectionObserver(i=>{i.forEach(r=>{r.isIntersecting&&(r.target.classList.add("visible","revealed"),e.unobserve(r.target))})},{threshold:.15});document.querySelectorAll("#about .section-title-text, #about .reveal-right, #about .reveal-left").forEach(i=>e.observe(i));let t=new IntersectionObserver(i=>{i.forEach(r=>{r.isIntersecting&&(r.target.classList.add("visible"),t.unobserve(r.target))})},{threshold:.15});document.querySelectorAll("#about .feature-item").forEach(i=>t.observe(i))})}scrollTo(e){document.querySelector(e)?.scrollIntoView({behavior:"smooth"})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-about-section"]],decls:45,vars:0,consts:[["titleRef",""],["textRef",""],["imageRef",""],["id","about",1,"about-section"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"text-align","center","margin-bottom","4rem"],[1,"badge-label"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","var(--color-text)"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(320px, 1fr))","gap","4rem","align-items","center"],[1,"reveal-right",2,"order","1"],[2,"font-size","1.05rem","line-height","2","color","rgba(26,60,94,0.8)","margin-bottom","2rem"],[2,"color","var(--color-accent)"],[1,"feature-item",2,"display","flex","align-items","flex-start","gap","1rem","margin-bottom","1.5rem",3,"transition-delay"],[1,"btn-primary",2,"margin-top","2rem",3,"click"],[1,"reveal-left",2,"order","2","display","flex","justify-content","center"],[1,"orbit-container"],[1,"orbit-core"],[1,"orbit-ring","orbit-ring-1"],[1,"orbit-item",2,"position","absolute","top","-22px","left","50%","margin-left","-22px","animation","spin 12s linear infinite reverse"],[1,"orbit-item",2,"position","absolute","top","50%","left","-22px","margin-top","-22px","animation","spin 14s linear infinite reverse"],[1,"orbit-item",2,"position","absolute","bottom","-22px","left","50%","margin-left","-22px","animation","spin 16s linear infinite reverse"],[1,"orbit-item",2,"position","absolute","top","50%","right","-22px","margin-top","-22px","animation","spin 18s linear infinite reverse"],[1,"orbit-ring","orbit-ring-2"],[1,"orbit-item",2,"position","absolute","top","-22px","left","50%","margin-left","-22px","animation","spin 20s linear infinite"],[1,"orbit-item",2,"position","absolute","bottom","-22px","left","50%","margin-left","-22px","animation","spin 23s linear infinite"],[1,"feature-item",2,"display","flex","align-items","flex-start","gap","1rem","margin-bottom","1.5rem"],[1,"feature-icon-wrap"],[2,"font-size","1.2rem"],[2,"font-weight","700","color","var(--color-text)","margin-bottom","0.2rem"],[2,"font-size","0.9rem","color","rgba(26,60,94,0.65)","line-height","1.6"]],template:function(t,i){t&1&&(U(0,"section",3)(1,"div",4)(2,"div",5)(3,"span",6),j(4,"\u0645\u0646 \u0646\u062D\u0646"),z(),U(5,"h2",7)(6,"div",8)(7,"span",9,0),j(9,"\u0631\u0648\u0627\u062F \u0635\u0646\u0627\u0639\u0629 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0641\u064A \u0627\u0644\u0645\u0646\u0637\u0642\u0629"),z()()()(),U(10,"div",10)(11,"div",11,1)(13,"p",12),j(14," \u062A\u0623\u0633\u0633 \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u0644\u0644\u0623\u062F\u0648\u064A\u0629 \u0645\u0646\u0630 \u0623\u0643\u062B\u0631 \u0645\u0646 "),U(15,"strong",13),j(16,"20 \u0639\u0627\u0645\u0627\u064B"),z(),j(17,"\u060C \u0648\u0623\u0635\u0628\u062D \u0627\u0644\u064A\u0648\u0645 \u0645\u0646 \u0623\u0628\u0631\u0632 \u0634\u0631\u0643\u0627\u062A \u062A\u0648\u0632\u064A\u0639 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0641\u064A \u0627\u0644\u0645\u0646\u0637\u0642\u0629. \u0646\u0641\u062E\u0631 \u0628\u062A\u0648\u0641\u064A\u0631 \u0623\u0643\u062B\u0631 \u0645\u0646 "),U(18,"strong",13),j(19,"500 \u0645\u0646\u062A\u062C \u062F\u0648\u0627\u0626\u064A"),z(),j(20," \u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u062C\u0648\u062F\u0629 \u0648\u0627\u0644\u0633\u0644\u0627\u0645\u0629\u060C \u062E\u062F\u0645\u0629\u064B \u0644\u0623\u0643\u062B\u0631 \u0645\u0646 \u0623\u0644\u0641 \u0639\u0645\u064A\u0644 \u0641\u064A \u0645\u062E\u062A\u0644\u0641 \u0627\u0644\u0642\u0637\u0627\u0639\u0627\u062A \u0627\u0644\u0635\u062D\u064A\u0629. "),z(),U(21,"div"),vt(22,SR,9,5,"div",14,MR),z(),U(24,"button",15),lt("click",function(){return i.scrollTo("#products")}),j(25,"\u0627\u0643\u062A\u0634\u0641 \u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"),z()(),U(26,"div",16,2)(28,"div",17)(29,"div",18),j(30,"\u{1F48A}"),z(),U(31,"div",19)(32,"div",20),j(33,"\u{1F9EC}"),z(),U(34,"div",21),j(35,"\u2697\uFE0F"),z(),U(36,"div",22),j(37,"\u{1FA7A}"),z(),U(38,"div",23),j(39,"\u{1F489}"),z()(),U(40,"div",24)(41,"div",25),j(42,"\u{1F3E5}"),z(),U(43,"div",26),j(44,"\u{1FA7B}"),z()()()()()()()),t&2&&(oe(22),yt(i.features))},encapsulation:2})};var wR=(n,e)=>e.id;function CR(n,e){if(n&1){let t=vn();U(0,"button",13),lt("click",function(r){let s=sn(t).$implicit,o=Tt();return o.setFilter(s),on(o.addRipple(r))}),j(1),z()}if(n&2){let t=e.$implicit,i=Tt();Ys("active",i.activeFilter()===t),oe(),ke(t)}}function IR(n,e){if(n&1&&(U(0,"div",15),j(1),z()),n&2){let t=Tt().$implicit;oe(),ke(t.badge)}}function AR(n,e){if(n&1){let t=vn();U(0,"div",14),ci(1,IR,2,1,"div",15),U(2,"div",16),j(3),z(),U(4,"h3",17),j(5),z(),U(6,"span",18),j(7),z(),U(8,"p",19),j(9),z(),U(10,"button",20),lt("click",function(r){sn(t);let s=Tt();return on(s.addRipple(r))}),j(11,"\u0627\u0644\u062A\u0641\u0627\u0635\u064A\u0644 \u2190"),z()()}if(n&2){let t=e.$implicit,i=e.$index;At("transition-delay",i*.1+"s"),oe(),li(t.badge?1:-1),oe(2),ke(t.icon),oe(2),ke(t.name),oe(2),ke(t.category),oe(2),ke(t.desc)}}var DR=["\u0627\u0644\u0643\u0644","\u0645\u0636\u0627\u062F\u0627\u062A \u062D\u064A\u0648\u064A\u0629","\u0645\u0643\u0645\u0644\u0627\u062A \u063A\u0630\u0627\u0626\u064A\u0629","\u0623\u062F\u0648\u064A\u0629 \u0642\u0644\u0628","\u062C\u0647\u0627\u0632 \u0647\u0636\u0645\u064A"],Gd=[{id:1,icon:"\u{1F48A}",name:"\u0623\u0645\u064A\u0643\u0633\u064A\u0644\u064A\u0646 500mg",desc:"\u0645\u0636\u0627\u062F \u062D\u064A\u0648\u064A \u0648\u0627\u0633\u0639 \u0627\u0644\u0645\u062C\u0627\u0644 \u0644\u0639\u0644\u0627\u062C \u0627\u0644\u0627\u0644\u062A\u0647\u0627\u0628\u0627\u062A \u0627\u0644\u0628\u0643\u062A\u064A\u0631\u064A\u0629",category:"\u0645\u0636\u0627\u062F\u0627\u062A \u062D\u064A\u0648\u064A\u0629",badge:"\u0627\u0644\u0623\u0643\u062B\u0631 \u0645\u0628\u064A\u0639\u0627\u064B"},{id:2,icon:"\u{1F9F4}",name:"\u0641\u064A\u062A\u0627\u0645\u064A\u0646 D3 + K2",desc:"\u0645\u0643\u0645\u0644 \u063A\u0630\u0627\u0626\u064A \u0645\u062A\u0643\u0627\u0645\u0644 \u0644\u0635\u062D\u0629 \u0627\u0644\u0639\u0638\u0627\u0645 \u0648\u0627\u0644\u0645\u0646\u0627\u0639\u0629",category:"\u0645\u0643\u0645\u0644\u0627\u062A \u063A\u0630\u0627\u0626\u064A\u0629",badge:"\u062C\u062F\u064A\u062F"},{id:3,icon:"\u2764\uFE0F",name:"\u0643\u0627\u0631\u062F\u064A\u0648\u0641\u0627\u0633\u062A 10mg",desc:"\u0644\u0639\u0644\u0627\u062C \u0627\u0631\u062A\u0641\u0627\u0639 \u0636\u063A\u0637 \u0627\u0644\u062F\u0645 \u0648\u0623\u0645\u0631\u0627\u0636 \u0627\u0644\u0642\u0644\u0628",category:"\u0623\u062F\u0648\u064A\u0629 \u0642\u0644\u0628",badge:null},{id:4,icon:"\u{1F33F}",name:"\u0628\u0631\u0648\u0628\u064A\u0648\u062A\u064A\u0643 \u0628\u0644\u0633",desc:"\u062A\u0631\u0643\u064A\u0628\u0629 \u0645\u062A\u0637\u0648\u0631\u0629 \u0644\u0635\u062D\u0629 \u0627\u0644\u062C\u0647\u0627\u0632 \u0627\u0644\u0647\u0636\u0645\u064A \u0648\u0627\u0644\u0623\u0645\u0639\u0627\u0621",category:"\u062C\u0647\u0627\u0632 \u0647\u0636\u0645\u064A",badge:null},{id:5,icon:"\u{1F489}",name:"\u0623\u0648\u0645\u064A\u063A\u0627 3 \u0641\u064A\u0634 \u0623\u0648\u064A\u0644",desc:"\u0632\u064A\u062A \u0627\u0644\u0633\u0645\u0643 \u0627\u0644\u0646\u0642\u064A \u0644\u0635\u062D\u0629 \u0627\u0644\u0642\u0644\u0628 \u0648\u0627\u0644\u0645\u0641\u0627\u0635\u0644 \u0648\u0627\u0644\u062F\u0645\u0627\u063A",category:"\u0645\u0643\u0645\u0644\u0627\u062A \u063A\u0630\u0627\u0626\u064A\u0629",badge:"\u0627\u0644\u0623\u0643\u062B\u0631 \u0645\u0628\u064A\u0639\u0627\u064B"},{id:6,icon:"\u{1F52C}",name:"\u0645\u064A\u062A\u0631\u0648\u0646\u064A\u062F\u0627\u0632\u0648\u0644 400mg",desc:"\u0644\u0639\u0644\u0627\u062C \u0627\u0644\u0627\u0644\u062A\u0647\u0627\u0628\u0627\u062A \u0648\u0627\u0644\u0639\u062F\u0648\u0649 \u0627\u0644\u062C\u0631\u062B\u0648\u0645\u064A\u0629 \u0627\u0644\u0645\u0639\u0648\u064A\u0629",category:"\u0645\u0636\u0627\u062F\u0627\u062A \u062D\u064A\u0648\u064A\u0629",badge:null}];function RR(n){let e=n.currentTarget,t=e.getBoundingClientRect(),i=document.createElement("span");i.className="ripple";let r=Math.max(t.width,t.height);i.style.cssText=`width:${r}px;height:${r}px;left:${n.clientX-t.left-r/2}px;top:${n.clientY-t.top-r/2}px;`,e.appendChild(i),setTimeout(()=>i.remove(),700)}var Wd=class n{categories=DR;products=Gd;activeFilter=Yt("\u0627\u0644\u0643\u0644");filtered=Yt([...Gd]);destroyRef=Ie(It);addRipple(e){RR(e)}setFilter(e){this.activeFilter.set(e),this.filtered.set(e==="\u0627\u0644\u0643\u0644"?[...Gd]:Gd.filter(t=>t.category===e))}constructor(){Qt(()=>{let e=new IntersectionObserver(([i])=>{i.isIntersecting&&(document.querySelector("#products .section-title-text")?.classList.add("revealed"),e.disconnect())},{threshold:.3}),t=document.querySelector("#products .section-title-text");t&&e.observe(t),Wc(i=>{this.filtered();let r=null,s=setTimeout(()=>{r=new IntersectionObserver(o=>{o.forEach(a=>{a.isIntersecting&&(a.target.classList.add("visible"),r?.unobserve(a.target))})},{threshold:.1}),document.querySelectorAll("#products .glass-card.reveal-up").forEach(o=>{o.classList.remove("visible"),r?.observe(o)})},50);i(()=>{clearTimeout(s),r?.disconnect()})}),this.destroyRef.onDestroy(()=>e.disconnect())})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-products-section"]],decls:18,vars:0,consts:[["titleRef",""],["id","products",1,"products-section"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"text-align","center","margin-bottom","3rem"],[1,"badge-label"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","var(--color-text)","margin-bottom","1rem"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"color","rgba(26,60,94,0.65)","font-size","1rem","max-width","550px","margin","0 auto"],[2,"display","flex","gap","0.75rem","justify-content","center","flex-wrap","wrap","margin-bottom","3rem"],[1,"filter-btn",3,"active"],[2,"display","grid","grid-template-columns","repeat(auto-fill, minmax(300px, 1fr))","gap","1.8rem"],[1,"glass-card","reveal-up",2,"padding","2rem",3,"transition-delay"],[1,"filter-btn",3,"click"],[1,"glass-card","reveal-up",2,"padding","2rem"],[2,"position","absolute","top","1rem","left","1rem","background","linear-gradient(135deg, var(--color-accent), var(--color-accent-dark))","color","white","font-size","0.72rem","font-weight","700","padding","0.2rem 0.8rem","border-radius","50px","box-shadow","0 3px 12px rgba(45,156,219,0.4)"],[1,"product-icon"],[2,"font-size","1.15rem","font-weight","700","color","var(--color-text)","margin-bottom","0.6rem"],[2,"display","inline-block","font-size","0.75rem","color","var(--color-accent)","background","rgba(45,156,219,0.08)","border","1px solid rgba(45,156,219,0.2)","border-radius","50px","padding","0.15rem 0.8rem","margin-bottom","0.8rem","font-weight","600"],[2,"font-size","0.9rem","color","rgba(26,60,94,0.65)","line-height","1.7","margin-bottom","1.5rem"],[1,"btn-accent",2,"width","100%",3,"click"]],template:function(t,i){t&1&&(U(0,"section",1)(1,"div",2)(2,"div",3)(3,"span",4),j(4,"\u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"),z(),U(5,"h2",5)(6,"div",6)(7,"span",7,0),j(9,"\u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627 \u0627\u0644\u062F\u0648\u0627\u0626\u064A\u0629 \u0627\u0644\u0645\u062A\u0645\u064A\u0632\u0629"),z()()(),U(10,"p",8),j(11,"\u0646\u0648\u0641\u0631 \u0645\u062C\u0645\u0648\u0639\u0629 \u0648\u0627\u0633\u0639\u0629 \u0645\u0646 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629 \u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u062C\u0648\u062F\u0629"),z()(),U(12,"div",9),vt(13,CR,2,3,"button",10,as),z(),U(15,"div",11),vt(16,AR,12,7,"div",12,wR),z()()()),t&2&&(oe(13),yt(i.categories),oe(3),yt(i.filtered()))},encapsulation:2})};var NR=["titleRef"],PR=["cardsRef"],LR=(n,e)=>e.title;function OR(n,e){if(n&1&&(U(0,"span"),j(1),z()),n&2){let t=e.$implicit,i=Tt().$implicit;wp(Cp("font-size: 0.78rem; font-weight: 600; padding: 0.25rem 0.85rem; border-radius: 50px; background: ",i.color,"15; border: 1px solid ",i.color,"35; color: ",i.color,";")),oe(),ke(t)}}function FR(n,e){if(n&1&&(U(0,"div",12,1)(2,"div",13),j(3),z(),U(4,"h3",14),j(5),z(),U(6,"p",15),j(7),z(),U(8,"div",16),vt(9,OR,2,6,"span",17,as),z()()),n&2){let t=e.$implicit,i=e.$index;At("transition-delay",t.delay+"s"),oe(2),At("animation-delay",i+"s")("background","linear-gradient(135deg, "+t.color+", "+t.color+"bb)"),oe(),Nn(" ",t.icon," "),oe(2),ke(t.title),oe(2),ke(t.desc),oe(2),yt(t.features)}}var kR=[{icon:"\u{1F69A}",title:"\u0627\u0644\u062A\u0648\u0632\u064A\u0639 \u0627\u0644\u062F\u0648\u0627\u0626\u064A",desc:"\u0634\u0628\u0643\u0629 \u062A\u0648\u0632\u064A\u0639 \u0645\u062A\u0637\u0648\u0631\u0629 \u062A\u0636\u0645\u0646 \u0648\u0635\u0648\u0644 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0641\u064A \u0627\u0644\u0648\u0642\u062A \u0627\u0644\u0645\u0646\u0627\u0633\u0628 \u0648\u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u0633\u0644\u0627\u0645\u0629 \u0648\u0627\u0644\u062C\u0648\u062F\u0629 \u0625\u0644\u0649 \u062C\u0645\u064A\u0639 \u0646\u0642\u0627\u0637 \u0627\u0644\u0628\u064A\u0639.",features:["\u062A\u063A\u0637\u064A\u0629 \u0634\u0627\u0645\u0644\u0629","\u062A\u0648\u0635\u064A\u0644 \u0645\u0628\u0631\u062F","\u062A\u062A\u0628\u0639 \u0645\u0628\u0627\u0634\u0631"],color:"#2D9CDB",delay:0},{icon:"\u{1F52C}",title:"\u0627\u0644\u0627\u0633\u062A\u0634\u0627\u0631\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629",desc:"\u0641\u0631\u064A\u0642 \u0645\u0646 \u0627\u0644\u0635\u064A\u0627\u062F\u0644\u0629 \u0648\u0627\u0644\u0645\u062E\u062A\u0635\u064A\u0646 \u0627\u0644\u0637\u0628\u064A\u064A\u0646 \u064A\u0642\u062F\u0645 \u0627\u0633\u062A\u0634\u0627\u0631\u0627\u062A \u0645\u062A\u062E\u0635\u0635\u0629 \u0648\u062F\u0639\u0645\u0627\u064B \u0639\u0644\u0645\u064A\u0627\u064B \u0645\u062A\u0648\u0627\u0635\u0644\u0627\u064B \u0644\u0644\u0639\u0645\u0644\u0627\u0621 \u0648\u0627\u0644\u0645\u0631\u0636\u0649.",features:["24/7 \u062F\u0639\u0645","\u0627\u0633\u062A\u0634\u0627\u0631\u0629 \u0645\u062C\u0627\u0646\u064A\u0629","\u062E\u0628\u0631\u0627\u0621 \u0645\u0639\u062A\u0645\u062F\u0648\u0646"],color:"#27AE60",delay:.15},{icon:"\u{1F3C6}",title:"\u0636\u0645\u0627\u0646 \u0627\u0644\u062C\u0648\u062F\u0629",desc:"\u0646\u0644\u062A\u0632\u0645 \u0628\u0645\u0639\u0627\u064A\u064A\u0631 \u062C\u0648\u062F\u0629 \u0639\u0627\u0644\u0645\u064A\u0629 \u0635\u0627\u0631\u0645\u0629 \u0641\u064A \u0643\u0644 \u0645\u0631\u0627\u062D\u0644 \u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0648\u0627\u0644\u062A\u0648\u0632\u064A\u0639\u060C \u0645\u0639 \u0634\u0647\u0627\u062F\u0627\u062A \u0627\u0639\u062A\u0645\u0627\u062F \u0645\u0646 \u0623\u0628\u0631\u0632 \u0627\u0644\u0647\u064A\u0626\u0627\u062A \u0627\u0644\u062F\u0648\u0644\u064A\u0629.",features:["\u0645\u0639\u062A\u0645\u062F \u062F\u0648\u0644\u064A\u0627\u064B","\u0641\u062D\u0635 \u062F\u0648\u0631\u064A","\u0634\u0647\u0627\u062F\u0627\u062A \u062C\u0648\u062F\u0629"],color:"#F2994A",delay:.3}],jd=class n{services=kR;titleRef=yn("titleRef");cardsRef=yn("cardsRef");destroyRef=Ie(It);constructor(){Qt(()=>{let e=new IntersectionObserver(([r])=>{r.isIntersecting&&(this.titleRef()?.nativeElement.classList.add("revealed"),e.disconnect())},{threshold:.3}),t=this.titleRef()?.nativeElement;t&&e.observe(t);let i=new IntersectionObserver(r=>{r.forEach(s=>{s.isIntersecting&&(s.target.classList.add("visible"),i.unobserve(s.target))})},{threshold:.15});document.querySelectorAll(".glass-card.reveal-up").forEach(r=>i.observe(r)),this.destroyRef.onDestroy(()=>{e.disconnect(),i.disconnect()})})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-services-section"]],viewQuery:function(t,i){t&1&&Zn(i.titleRef,NR,5)(i.cardsRef,PR,5),t&2&&ui(2)},decls:15,vars:0,consts:[["titleRef",""],["cardsRef",""],["id","services",1,"services-section"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"text-align","center","margin-bottom","4rem"],[1,"badge-label"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","var(--color-text)","margin-bottom","1rem"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"color","rgba(26,60,94,0.65)","font-size","1rem","max-width","550px","margin","0 auto"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(300px, 1fr))","gap","2rem"],[1,"glass-card","reveal-up",2,"padding","2.5rem 2rem","text-align","center",3,"transition-delay"],[1,"glass-card","reveal-up",2,"padding","2.5rem 2rem","text-align","center"],[1,"service-icon-animated"],[2,"font-size","1.35rem","font-weight","800","color","var(--color-text)","margin-bottom","1rem"],[2,"font-size","0.95rem","color","rgba(26,60,94,0.65)","line-height","1.8","margin-bottom","1.5rem"],[2,"display","flex","gap","0.6rem","justify-content","center","flex-wrap","wrap"],[3,"style"]],template:function(t,i){t&1&&(U(0,"section",2)(1,"div",3)(2,"div",4)(3,"span",5),j(4,"\u062E\u062F\u0645\u0627\u062A\u0646\u0627"),z(),U(5,"h2",6)(6,"div",7)(7,"span",8,0),j(9,"\u062E\u062F\u0645\u0627\u062A \u0645\u062A\u0643\u0627\u0645\u0644\u0629 \u0644\u0642\u0637\u0627\u0639 \u0627\u0644\u0635\u062D\u0629"),z()()(),U(10,"p",9),j(11,"\u0646\u0642\u062F\u0645 \u062D\u0644\u0648\u0644\u0627\u064B \u0634\u0627\u0645\u0644\u0629 \u062A\u063A\u0637\u064A \u0643\u0644 \u0627\u062D\u062A\u064A\u0627\u062C\u0627\u062A \u0642\u0637\u0627\u0639 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0631\u0639\u0627\u064A\u0629 \u0627\u0644\u0635\u062D\u064A\u0629"),z()(),U(12,"div",10),vt(13,FR,11,9,"div",11,LR),z()()()),t&2&&(oe(13),yt(i.services))},encapsulation:2})};var UR=["titleRef"],BR=(n,e)=>e.name;function VR(n,e){if(n&1&&(U(0,"div",10)(1,"span",14),j(2),z(),U(3,"div")(4,"div",15),j(5),z(),U(6,"div",16),j(7),z()()()),n&2){let t=e.$implicit;oe(2),ke(t.icon),oe(3),ke(t.name),oe(2),ke(t.country)}}var sE=[{name:"PharmaCo",icon:"\u{1F48A}",country:"\u0623\u0644\u0645\u0627\u0646\u064A\u0627"},{name:"MediLife",icon:"\u{1F3E5}",country:"\u0641\u0631\u0646\u0633\u0627"},{name:"BioCure",icon:"\u{1F9EC}",country:"\u0627\u0644\u0648\u0644\u0627\u064A\u0627\u062A \u0627\u0644\u0645\u062A\u062D\u062F\u0629"},{name:"HealthPlus",icon:"\u2795",country:"\u0628\u0631\u064A\u0637\u0627\u0646\u064A\u0627"},{name:"VitaGroup",icon:"\u{1F33F}",country:"\u0633\u0648\u064A\u0633\u0631\u0627"},{name:"NovaMed",icon:"\u2695\uFE0F",country:"\u0625\u064A\u0637\u0627\u0644\u064A\u0627"},{name:"GenoPharma",icon:"\u{1F52C}",country:"\u0643\u0646\u062F\u0627"},{name:"SafeRx",icon:"\u{1F6E1}\uFE0F",country:"\u0647\u0648\u0644\u0646\u062F\u0627"}],HR=[...sE,...sE],$d=class n{doubled=HR;titleRef=yn("titleRef");destroyRef=Ie(It);constructor(){Qt(()=>{let e=new IntersectionObserver(([i])=>{i.isIntersecting&&(this.titleRef()?.nativeElement.classList.add("revealed"),e.disconnect())},{threshold:.3}),t=this.titleRef()?.nativeElement;t&&e.observe(t),this.destroyRef.onDestroy(()=>e.disconnect())})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-partners-section"]],viewQuery:function(t,i){t&1&&Zn(i.titleRef,UR,5),t&2&&ui()},decls:31,vars:0,consts:[["titleRef",""],["id","partners",1,"partners-section"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem","text-align","center"],[1,"badge-label",2,"color","rgba(184,228,249,0.8)","border-color","rgba(184,228,249,0.25)","background","rgba(184,228,249,0.05)"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","#B8E4F9","margin-bottom","1rem"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"color","rgba(184,228,249,0.6)","font-size","1rem","max-width","550px","margin","0 auto 3.5rem"],[2,"overflow","hidden","padding","1rem 0"],[1,"marquee-track"],[1,"partner-logo-item"],[2,"max-width","900px","margin","4rem auto 0","padding","0 2rem","display","grid","grid-template-columns","repeat(auto-fit, minmax(200px, 1fr))","gap","2rem","text-align","center"],[2,"font-size","2rem","font-weight","900","background","linear-gradient(135deg, #B8E4F9, #2D9CDB)","-webkit-background-clip","text","-webkit-text-fill-color","transparent","background-clip","text","margin-bottom","0.4rem"],[2,"color","rgba(184,228,249,0.6)","font-size","0.9rem"],[2,"font-size","1.8rem","filter","drop-shadow(0 0 8px rgba(45,156,219,0.5))"],[2,"color","#B8E4F9","font-weight","700","font-size","0.95rem"],[2,"color","rgba(184,228,249,0.5)","font-size","0.75rem"]],template:function(t,i){t&1&&(U(0,"section",1)(1,"div",2)(2,"span",3),j(3,"\u0634\u0631\u0643\u0627\u0624\u0646\u0627"),z(),U(4,"h2",4)(5,"div",5)(6,"span",6,0),j(8,"\u0634\u0631\u0643\u0627\u0624\u0646\u0627 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645"),z()()(),U(9,"p",7),j(10," \u0646\u062A\u0639\u0627\u0648\u0646 \u0645\u0639 \u0623\u0628\u0631\u0632 \u0634\u0631\u0643\u0627\u062A \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0627\u0644\u0639\u0627\u0644\u0645\u064A\u0629 \u0644\u0646\u0648\u0641\u0631 \u0644\u0643\u0645 \u0623\u0641\u0636\u0644 \u0627\u0644\u0645\u0646\u062A\u062C\u0627\u062A "),z()(),U(11,"div",8)(12,"div",9),vt(13,VR,8,3,"div",10,BR),z()(),U(15,"div",11)(16,"div")(17,"div",12),j(18,"50+"),z(),U(19,"div",13),j(20,"\u062F\u0648\u0644\u0629 \u062D\u0648\u0644 \u0627\u0644\u0639\u0627\u0644\u0645"),z()(),U(21,"div")(22,"div",12),j(23,"15+"),z(),U(24,"div",13),j(25,"\u0633\u0646\u0629 \u0634\u0631\u0627\u0643\u0629 \u0645\u062A\u0648\u0627\u0635\u0644\u0629"),z()(),U(26,"div")(27,"div",12),j(28,"99%"),z(),U(29,"div",13),j(30,"\u0631\u0636\u0627 \u0627\u0644\u0634\u0631\u0643\u0627\u0621"),z()()()()),t&2&&(oe(13),yt(i.doubled))},encapsulation:2})};var oE=(n,e)=>e.name;function zR(n,e){if(n&1){let t=vn();U(0,"button",20),lt("click",function(){let r=sn(t).$index,s=Tt();return on(s.goTo(r))}),z()}if(n&2){let t=e.$index,i=Tt();At("width",t===i.current()?"2rem":"0.55rem")("height","0.55rem")("border-radius","50px")("border","none")("background",t===i.current()?"var(--color-accent)":"rgba(45,156,219,0.25)")("cursor","none")("transition","all 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94)")("box-shadow",t===i.current()?"0 2px 12px rgba(45,156,219,0.5)":"none")}}function GR(n,e){if(n&1){let t=vn();U(0,"div",21),lt("click",function(){let r=sn(t).$index,s=Tt();return on(s.goTo(r))}),U(1,"div",22)(2,"span",23),j(3),z(),U(4,"div")(5,"div",24),j(6),z(),U(7,"div",25),j(8),z()()()()}if(n&2){let t=e.$implicit,i=e.$index,r=Tt();At("background",i===r.current()?"rgba(45,156,219,0.1)":"rgba(255,255,255,0.6)")("border","1px solid "+(i===r.current()?"rgba(45,156,219,0.4)":"rgba(184,228,249,0.3)"))("border-radius","14px")("padding","1rem 1.2rem")("cursor","none")("transition","all 0.3s ease")("transition-delay",i*.1+"s")("backdrop-filter","blur(8px)"),oe(3),ke(t.avatar),oe(3),ke(t.name),oe(2),ke("\u2605".repeat(t.stars))}}var cg=[{name:"\u062F. \u0623\u062D\u0645\u062F \u0627\u0644\u0631\u0634\u064A\u062F\u064A",role:"\u0645\u062F\u064A\u0631 \u0645\u0633\u062A\u0634\u0641\u0649 \u0627\u0644\u0623\u0645\u0644",stars:5,text:"\u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u0634\u0631\u064A\u0643\u0646\u0627 \u0627\u0644\u0645\u0648\u062B\u0648\u0642 \u0645\u0646\u0630 \u0633\u0646\u0648\u0627\u062A. \u062C\u0648\u062F\u0629 \u0645\u0646\u062A\u062C\u0627\u062A\u0647\u0645 \u0644\u0627 \u062A\u0636\u0627\u0647\u0649 \u0648\u0633\u0631\u0639\u0629 \u0627\u0644\u062A\u0648\u0635\u064A\u0644 \u062A\u0641\u0648\u0642 \u0627\u0644\u062A\u0648\u0642\u0639\u0627\u062A. \u0646\u0646\u0635\u062D \u0628\u0647\u0645 \u0644\u0643\u0644 \u0627\u0644\u0645\u0624\u0633\u0633\u0627\u062A \u0627\u0644\u0635\u062D\u064A\u0629.",avatar:"\u{1F468}\u200D\u2695\uFE0F"},{name:"\u0627\u0644\u0635\u064A\u062F\u0644\u0627\u0646\u064A\u0629 \u0641\u0627\u0637\u0645\u0629 \u0627\u0644\u0639\u0645\u0631\u064A",role:"\u0645\u0627\u0644\u0643\u0629 \u0635\u064A\u062F\u0644\u064A\u0629 \u0627\u0644\u0639\u0627\u0641\u064A\u0629",stars:5,text:"\u062A\u0639\u0627\u0645\u0644\u0646\u0627 \u0645\u0639 \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u0645\u0646\u0630 \u0623\u0643\u062B\u0631 \u0645\u0646 8 \u0633\u0646\u0648\u0627\u062A. \u0627\u0644\u062A\u0632\u0627\u0645\u0647\u0645 \u0628\u0627\u0644\u062C\u0648\u062F\u0629 \u0648\u0627\u0644\u0623\u0633\u0639\u0627\u0631 \u0627\u0644\u062A\u0646\u0627\u0641\u0633\u064A\u0629 \u062C\u0639\u0644\u0646\u0627 \u0634\u0631\u0643\u0627\u0621 \u0646\u0627\u062C\u062D\u064A\u0646 \u0639\u0644\u0649 \u0627\u0644\u0645\u062F\u0649 \u0627\u0644\u0637\u0648\u064A\u0644.",avatar:"\u{1F469}\u200D\u2695\uFE0F"},{name:"\u0645. \u062E\u0627\u0644\u062F \u0627\u0644\u0633\u0627\u0644\u0645",role:"\u0645\u062F\u064A\u0631 \u0645\u0634\u062A\u0631\u064A\u0627\u062A \u0645\u062C\u0645\u0648\u0639\u0629 \u0637\u0628\u064A\u0629",stars:5,text:"\u0646\u0642\u062F\u0631 \u062B\u0642\u062A\u0646\u0627 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0641\u064A \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628. \u062A\u0634\u0643\u064A\u0644\u0629 \u0645\u0646\u062A\u062C\u0627\u062A\u0647\u0645 \u0627\u0644\u0648\u0627\u0633\u0639\u0629 \u0648\u062E\u062F\u0645\u0629 \u0627\u0644\u0639\u0645\u0644\u0627\u0621 \u0627\u0644\u0645\u0645\u062A\u0627\u0632\u0629 \u062A\u062C\u0639\u0644\u0647\u0645 \u0627\u0644\u062E\u064A\u0627\u0631 \u0627\u0644\u0623\u0648\u0644 \u062F\u0627\u0626\u0645\u0627\u064B.",avatar:"\u{1F468}\u200D\u{1F4BC}"},{name:"\u062F. \u0646\u0648\u0631\u0629 \u0627\u0644\u062D\u0631\u0628\u064A",role:"\u0637\u0628\u064A\u0628\u0629 \u0639\u0627\u0645\u0629",stars:5,text:"\u0623\u0631\u0634\u062D \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u0644\u0643\u0644 \u0632\u0645\u0644\u0627\u0626\u064A \u0627\u0644\u0623\u0637\u0628\u0627\u0621. \u0645\u0646\u062A\u062C\u0627\u062A\u0647\u0645 \u0645\u0639\u062A\u0645\u062F\u0629 \u0648\u0645\u0636\u0645\u0648\u0646\u0629\u060C \u0648\u0641\u0631\u064A\u0642\u0647\u0645 \u0627\u0644\u0627\u0633\u062A\u0634\u0627\u0631\u064A \u0639\u0644\u0649 \u0642\u062F\u0631 \u0639\u0627\u0644\u064D \u0645\u0646 \u0627\u0644\u0627\u062D\u062A\u0631\u0627\u0641\u064A\u0629.",avatar:"\u{1F469}\u200D\u2695\uFE0F"}],qd=class n{testimonials=cg;current=Yt(0);isAnimating=Yt(!1);destroyRef=Ie(It);autoInterval=null;goTo(e){this.isAnimating()||e===this.current()||(this.isAnimating.set(!0),this.current.set(e),setTimeout(()=>this.isAnimating.set(!1),500),this.autoInterval&&clearInterval(this.autoInterval),this.autoInterval=setInterval(()=>{this.current.update(t=>(t+1)%cg.length)},5e3))}constructor(){Qt(()=>{let e=new IntersectionObserver(([r])=>{r.isIntersecting&&(document.querySelector("#testimonials .section-title-text")?.classList.add("revealed"),e.disconnect())},{threshold:.3}),t=document.querySelector("#testimonials .section-title-text");t&&e.observe(t);let i=new IntersectionObserver(r=>{r.forEach(s=>{s.isIntersecting&&s.target.classList.add("visible")})},{threshold:.1});document.querySelectorAll("#testimonials .reveal-up").forEach(r=>i.observe(r)),this.autoInterval=setInterval(()=>{this.current.update(r=>(r+1)%cg.length)},5e3),this.destroyRef.onDestroy(()=>{e.disconnect(),i.disconnect(),this.autoInterval&&clearInterval(this.autoInterval)})})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-testimonials-section"]],decls:30,vars:9,consts:[["titleRef",""],["id","testimonials",1,"testimonials-section"],[2,"max-width","1100px","margin","0 auto","padding","0 2rem"],[2,"text-align","center","margin-bottom","4rem"],[1,"badge-label"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","var(--color-text)"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"position","relative","margin-bottom","2.5rem"],[2,"background","rgba(255,255,255,0.85)","backdrop-filter","blur(12px)","border","1px solid rgba(184,228,249,0.4)","border-radius","24px","padding","clamp(2rem, 5vw, 3.5rem)","text-align","center","box-shadow","0 20px 60px rgba(45,156,219,0.12)","transition","opacity 0.4s ease, transform 0.4s ease","position","relative","overflow","hidden"],[2,"position","absolute","top","-60px","left","-60px","width","200px","height","200px","border-radius","50%","background","radial-gradient(circle, rgba(45,156,219,0.06) 0%, transparent 70%)","pointer-events","none"],[2,"width","80px","height","80px","border-radius","50%","background","linear-gradient(135deg, rgba(45,156,219,0.15), rgba(184,228,249,0.1))","border","2px solid rgba(45,156,219,0.3)","display","flex","align-items","center","justify-content","center","font-size","2.5rem","margin","0 auto 1.2rem","box-shadow","0 6px 24px rgba(45,156,219,0.2)"],[1,"stars",2,"margin-bottom","1rem"],[2,"font-size","clamp(1rem, 2.5vw, 1.15rem)","color","rgba(26,60,94,0.8)","line-height","1.9","max-width","650px","margin","0 auto 1.5rem","font-style","italic"],[2,"font-weight","800","font-size","1.05rem","color","var(--color-text)"],[2,"font-size","0.85rem","color","var(--color-accent)","font-weight","500","margin-top","0.2rem"],[2,"display","flex","justify-content","center","gap","0.6rem","margin-bottom","2rem"],[3,"width","height","border-radius","border","background","cursor","transition","box-shadow"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(200px, 1fr))","gap","1rem"],[1,"reveal-up",3,"background","border","border-radius","padding","cursor","transition","transition-delay","backdrop-filter"],[3,"click"],[1,"reveal-up",3,"click"],[2,"display","flex","align-items","center","gap","0.7rem"],[2,"font-size","1.5rem"],[2,"font-weight","700","font-size","0.88rem","color","var(--color-text)"],[1,"stars",2,"font-size","0.8rem"]],template:function(t,i){t&1&&(U(0,"section",1)(1,"div",2)(2,"div",3)(3,"span",4),j(4,"\u0622\u0631\u0627\u0621 \u0627\u0644\u0639\u0645\u0644\u0627\u0621"),z(),U(5,"h2",5)(6,"div",6)(7,"span",7,0),j(9,"\u0645\u0627\u0630\u0627 \u064A\u0642\u0648\u0644 \u0639\u0645\u0644\u0627\u0624\u0646\u0627"),z()()()(),U(10,"div",8)(11,"div",9),an(12,"div",10),U(13,"div",11),j(14),z(),U(15,"div",12),j(16),z(),U(17,"p",13),j(18),z(),U(19,"div")(20,"div",14),j(21),z(),U(22,"div",15),j(23),z()()()(),U(24,"div",16),vt(25,zR,1,16,"button",17,oE),z(),U(27,"div",18),vt(28,GR,9,19,"div",19,oE),z()()()),t&2&&(oe(11),At("opacity",i.isAnimating()?0:1)("transform",i.isAnimating()?"translateY(10px)":"translateY(0)"),oe(3),ke(i.testimonials[i.current()].avatar),oe(2),ke("\u2605".repeat(i.testimonials[i.current()].stars)),oe(2),Nn('"',i.testimonials[i.current()].text,'"'),oe(3),ke(i.testimonials[i.current()].name),oe(2),ke(i.testimonials[i.current()].role),oe(2),yt(i.testimonials),oe(3),yt(i.testimonials))},encapsulation:2})};var WR=(n,e)=>e.label;function jR(n,e){n&1&&(U(0,"div",14),j(1,"\u2705 \u062A\u0645 \u0625\u0631\u0633\u0627\u0644 \u0631\u0633\u0627\u0644\u062A\u0643 \u0628\u0646\u062C\u0627\u062D! \u0633\u0646\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0643 \u0642\u0631\u064A\u0628\u0627\u064B."),z())}function $R(n,e){if(n&1&&(U(0,"div",35)(1,"div",36),j(2),z(),U(3,"div")(4,"div",37),j(5),z(),U(6,"div",38),j(7),z()()()),n&2){let t=e.$implicit,i=e.$index;At("transition-delay",i*.1+"s"),oe(2),ke(t.icon),oe(3),ke(t.label),oe(2),ke(t.value)}}var qR=[{icon:"\u{1F4CD}",label:"\u0627\u0644\u0639\u0646\u0648\u0627\u0646",value:"\u0627\u0644\u0631\u064A\u0627\u0636\u060C \u0627\u0644\u0645\u0645\u0644\u0643\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0627\u0644\u0633\u0639\u0648\u062F\u064A\u0629\u060C \u062D\u064A \u0627\u0644\u0645\u0644\u0632"},{icon:"\u{1F4DE}",label:"\u0627\u0644\u0647\u0627\u062A\u0641",value:"+966 11 234 5678"},{icon:"\u{1F4E7}",label:"\u0627\u0644\u0628\u0631\u064A\u062F \u0627\u0644\u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A",value:"info@alamin-group.com"},{icon:"\u{1F550}",label:"\u0633\u0627\u0639\u0627\u062A \u0627\u0644\u0639\u0645\u0644",value:"\u0627\u0644\u0623\u062D\u062F - \u0627\u0644\u062E\u0645\u064A\u0633: 8\u0635 - 6\u0645"}];function XR(n){let e=n.currentTarget,t=e.getBoundingClientRect(),i=document.createElement("span");i.className="ripple";let r=Math.max(t.width,t.height);i.style.cssText=`width:${r}px;height:${r}px;left:${n.clientX-t.left-r/2}px;top:${n.clientY-t.top-r/2}px;`,e.appendChild(i),setTimeout(()=>i.remove(),700)}var Xd=class n{contactInfo=qR;form=Yt({name:"",email:"",phone:"",message:""});sent=Yt(!1);addRipple(e){XR(e)}updateField(e,t){let i=t.target.value;this.form.update(r=>Vt(St({},r),{[e]:i}))}handleSubmit(){this.sent.set(!0),this.form.set({name:"",email:"",phone:"",message:""}),setTimeout(()=>this.sent.set(!1),4e3)}constructor(){Qt(()=>{let e=new IntersectionObserver(t=>{t.forEach(i=>{i.isIntersecting&&(i.target.classList.add("visible","revealed"),e.unobserve(i.target))})},{threshold:.15});document.querySelectorAll("#contact .section-title-text, #contact .reveal-right, #contact .reveal-left").forEach(t=>e.observe(t))})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-contact-section"]],decls:60,vars:5,consts:[["titleRef",""],["formRef",""],["infoRef",""],["id","contact",1,"contact-section"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"text-align","center","margin-bottom","4rem"],[1,"badge-label"],[2,"font-size","clamp(1.8rem, 4vw, 2.8rem)","font-weight","900","color","var(--color-text)"],[1,"section-title-wrap"],[1,"section-title-text"],[2,"color","rgba(26,60,94,0.65)","font-size","1rem","max-width","500px","margin","1rem auto 0"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(320px, 1fr))","gap","3rem","align-items","start"],[1,"reveal-right","glass-card",2,"padding","2.5rem"],[2,"font-weight","800","font-size","1.3rem","color","var(--color-text)","margin-bottom","1.8rem"],[2,"background","rgba(39,174,96,0.1)","border","1px solid rgba(39,174,96,0.3)","border-radius","12px","padding","1rem 1.2rem","margin-bottom","1.5rem","color","#27AE60","font-weight","600","font-size","0.95rem","display","flex","align-items","center","gap","0.6rem"],[3,"ngSubmit"],[1,"floating-input-wrap"],["type","text","placeholder"," ","required","",1,"floating-input",3,"input","value"],[1,"floating-label"],["type","email","placeholder"," ","required","",1,"floating-input",3,"input","value"],["type","tel","placeholder"," ",1,"floating-input",2,"direction","ltr","text-align","right",3,"input","value"],["placeholder"," ","required","",1,"floating-input","floating-textarea",3,"input","value"],["type","submit",1,"btn-primary",2,"width","100%","font-size","1.05rem","padding","1rem",3,"click"],[1,"reveal-left"],[1,"contact-info-item",3,"transition-delay"],[2,"margin-top","1.5rem","border-radius","16px","overflow","hidden","border","1px solid rgba(184,228,249,0.35)","background","linear-gradient(135deg, rgba(45,156,219,0.06), rgba(184,228,249,0.04))","height","180px","display","flex","align-items","center","justify-content","center","flex-direction","column","gap","0.5rem"],[2,"font-size","3rem"],[2,"color","rgba(26,60,94,0.5)","font-size","0.9rem"],[2,"margin-top","1.5rem"],[2,"color","rgba(26,60,94,0.6)","font-size","0.9rem","margin-bottom","0.8rem"],[2,"display","flex","gap","0.7rem"],["title","Facebook",1,"social-btn"],["title","Instagram",1,"social-btn"],["title","Twitter",1,"social-btn"],["title","LinkedIn",1,"social-btn"],[1,"contact-info-item"],[1,"contact-icon"],[2,"font-size","0.78rem","color","rgba(26,60,94,0.5)","font-weight","600","margin-bottom","0.15rem"],[2,"font-weight","600","color","var(--color-text)","font-size","0.95rem"]],template:function(t,i){t&1&&(U(0,"section",3)(1,"div",4)(2,"div",5)(3,"span",6),j(4,"\u0627\u062A\u0635\u0644 \u0628\u0646\u0627"),z(),U(5,"h2",7)(6,"div",8)(7,"span",9,0),j(9,"\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0646\u0627 \u0627\u0644\u064A\u0648\u0645"),z()()(),U(10,"p",10),j(11,"\u0641\u0631\u064A\u0642\u0646\u0627 \u062C\u0627\u0647\u0632 \u0644\u0644\u0625\u062C\u0627\u0628\u0629 \u0639\u0644\u0649 \u062C\u0645\u064A\u0639 \u0627\u0633\u062A\u0641\u0633\u0627\u0631\u0627\u062A\u0643\u0645 \u0648\u062A\u0642\u062F\u064A\u0645 \u0623\u0641\u0636\u0644 \u0627\u0644\u062D\u0644\u0648\u0644"),z()(),U(12,"div",11)(13,"div",12,1)(15,"h3",13),j(16,"\u0623\u0631\u0633\u0644 \u0631\u0633\u0627\u0644\u062A\u0643"),z(),ci(17,jR,2,0,"div",14),U(18,"form",15),lt("ngSubmit",function(){return i.handleSubmit()}),U(19,"div",16)(20,"input",17),lt("input",function(s){return i.updateField("name",s)}),z(),U(21,"label",18),j(22,"\u0627\u0644\u0627\u0633\u0645 \u0627\u0644\u0643\u0627\u0645\u0644 *"),z()(),U(23,"div",16)(24,"input",19),lt("input",function(s){return i.updateField("email",s)}),z(),U(25,"label",18),j(26,"\u0627\u0644\u0628\u0631\u064A\u062F \u0627\u0644\u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A *"),z()(),U(27,"div",16)(28,"input",20),lt("input",function(s){return i.updateField("phone",s)}),z(),U(29,"label",18),j(30,"\u0631\u0642\u0645 \u0627\u0644\u0647\u0627\u062A\u0641 (\u0627\u062E\u062A\u064A\u0627\u0631\u064A)"),z()(),U(31,"div",16)(32,"textarea",21),lt("input",function(s){return i.updateField("message",s)}),z(),U(33,"label",18),j(34,"\u0631\u0633\u0627\u0644\u062A\u0643 *"),z()(),U(35,"button",22),lt("click",function(s){return i.addRipple(s)}),j(36,"\u0625\u0631\u0633\u0627\u0644 \u0627\u0644\u0631\u0633\u0627\u0644\u0629 \u2709\uFE0F"),z()()(),U(37,"div",23,2)(39,"h3",13),j(40,"\u0645\u0639\u0644\u0648\u0645\u0627\u062A \u0627\u0644\u062A\u0648\u0627\u0635\u0644"),z(),vt(41,$R,8,5,"div",24,WR),U(43,"div",25)(44,"span",26),j(45,"\u{1F4CD}"),z(),U(46,"span",27),j(47,"\u0627\u0644\u0631\u064A\u0627\u0636\u060C \u0627\u0644\u0645\u0645\u0644\u0643\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0627\u0644\u0633\u0639\u0648\u062F\u064A\u0629"),z()(),U(48,"div",28)(49,"p",29),j(50,"\u062A\u0627\u0628\u0639\u0648\u0646\u0627 \u0639\u0644\u0649:"),z(),U(51,"div",30)(52,"button",31),j(53,"\u{1F4D8}"),z(),U(54,"button",32),j(55,"\u{1F4F8}"),z(),U(56,"button",33),j(57,"\u{1F426}"),z(),U(58,"button",34),j(59,"\u{1F4BC}"),z()()()()()()()),t&2&&(oe(17),li(i.sent()?17:-1),oe(3),Rn("value",i.form().name),oe(4),Rn("value",i.form().email),oe(4),Rn("value",i.form().phone),oe(4),Rn("value",i.form().message),oe(9),yt(i.contactInfo))},encapsulation:2})};var YR=(n,e)=>e.name,ZR=(n,e)=>e.href,JR=(n,e)=>e.text;function KR(n,e){if(n&1&&(U(0,"button",9),j(1),z()),n&2){let t=e.$implicit;Rn("title",t.name),oe(),ke(t.icon)}}function QR(n,e){if(n&1){let t=vn();U(0,"li",12)(1,"a",18),lt("click",function(r){let s=sn(t).$implicit,o=Tt();return on(o.scrollTo(r,s.href))}),U(2,"span",19),j(3,"\u2190"),z(),j(4),z()()}if(n&2){let t=e.$implicit;oe(),Rn("href",t.href,Xs),oe(3),Nn("",t.label," ")}}function eN(n,e){if(n&1&&(U(0,"li",12)(1,"span",20)(2,"span",21),j(3,"\u25CF"),z(),j(4),z()()),n&2){let t=e.$implicit;oe(4),Nn("",t," ")}}function tN(n,e){if(n&1&&(U(0,"div",13)(1,"span",22),j(2),z(),U(3,"span",23),j(4),z()()),n&2){let t=e.$implicit;oe(2),ke(t.icon),oe(2),ke(t.text)}}var nN=[{href:"#hero",label:"\u0627\u0644\u0631\u0626\u064A\u0633\u064A\u0629"},{href:"#about",label:"\u0645\u0646 \u0646\u062D\u0646"},{href:"#products",label:"\u0645\u0646\u062A\u062C\u0627\u062A\u0646\u0627"},{href:"#services",label:"\u062E\u062F\u0645\u0627\u062A\u0646\u0627"},{href:"#partners",label:"\u0627\u0644\u0634\u0631\u0643\u0627\u0621"},{href:"#contact",label:"\u0627\u062A\u0635\u0644 \u0628\u0646\u0627"}],iN=[{icon:"\u{1F4D8}",name:"Facebook"},{icon:"\u{1F4F8}",name:"Instagram"},{icon:"\u{1F426}",name:"Twitter/X"},{icon:"\u{1F4BC}",name:"LinkedIn"},{icon:"\u{1F4F1}",name:"WhatsApp"}],rN=["\u0627\u0644\u062A\u0648\u0632\u064A\u0639 \u0627\u0644\u062F\u0648\u0627\u0626\u064A","\u0627\u0644\u0627\u0633\u062A\u0634\u0627\u0631\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629","\u0636\u0645\u0627\u0646 \u0627\u0644\u062C\u0648\u062F\u0629","\u0627\u0644\u0634\u0631\u0627\u0643\u0627\u062A \u0627\u0644\u062F\u0648\u0644\u064A\u0629","\u0627\u0644\u062A\u062E\u0632\u064A\u0646 \u0627\u0644\u0645\u0628\u0631\u062F","\u0627\u0644\u062F\u0639\u0645 \u0627\u0644\u0641\u0646\u064A"],sN=[{icon:"\u{1F4CD}",text:"\u0627\u0644\u0631\u064A\u0627\u0636\u060C \u0627\u0644\u0645\u0645\u0644\u0643\u0629 \u0627\u0644\u0639\u0631\u0628\u064A\u0629 \u0627\u0644\u0633\u0639\u0648\u062F\u064A\u0629"},{icon:"\u{1F4DE}",text:"+966 11 234 5678"},{icon:"\u{1F4E7}",text:"info@alamin-group.com"},{icon:"\u{1F550}",text:"\u0627\u0644\u0623\u062D\u062F - \u0627\u0644\u062E\u0645\u064A\u0633: 8\u0635 - 6\u0645"}],Yd=class n{quickLinks=nN;socialLinks=iN;services=rN;contactItems=sN;currentYear=new Date().getFullYear();scrollTo(e,t){e.preventDefault(),document.querySelector(t)?.scrollIntoView({behavior:"smooth"})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-footer"]],decls:44,vars:1,consts:[[1,"footer"],[2,"max-width","1200px","margin","0 auto","padding","0 2rem"],[2,"display","grid","grid-template-columns","repeat(auto-fit, minmax(220px, 1fr))","gap","3rem","margin-bottom","3rem","padding-bottom","3rem","border-bottom","1px solid rgba(184,228,249,0.1)"],[2,"display","flex","align-items","center","gap","0.7rem","margin-bottom","1.2rem"],[2,"width","44px","height","44px","border-radius","12px","background","linear-gradient(135deg, #2D9CDB, #1a7ab5)","display","flex","align-items","center","justify-content","center","font-size","1.4rem","box-shadow","0 4px 15px rgba(45,156,219,0.4)"],[2,"color","#B8E4F9","font-weight","900","font-size","1.1rem"],[2,"color","rgba(184,228,249,0.45)","font-size","0.7rem"],[2,"color","rgba(184,228,249,0.55)","font-size","0.88rem","line-height","1.9","margin-bottom","1.5rem"],[2,"display","flex","gap","0.6rem","flex-wrap","wrap"],[1,"social-btn",2,"width","36px","height","36px","font-size","1rem",3,"title"],[2,"color","#B8E4F9","font-weight","700","font-size","1rem","margin-bottom","1.2rem"],[2,"list-style","none","padding","0","margin","0"],[2,"margin-bottom","0.7rem"],[2,"display","flex","align-items","flex-start","gap","0.6rem","margin-bottom","0.9rem"],[2,"display","flex","flex-wrap","wrap","justify-content","space-between","align-items","center","gap","1rem"],[2,"color","rgba(184,228,249,0.4)","font-size","0.82rem"],[2,"display","flex","gap","1.5rem"],["href","#",1,"footer-link",2,"font-size","0.8rem"],[1,"footer-link",2,"display","flex","align-items","center","gap","0.4rem",3,"click","href"],[2,"color","var(--color-accent)","font-size","0.7rem"],[1,"footer-link"],[2,"color","var(--color-accent)","margin-left","0.4rem"],[2,"font-size","0.95rem","flex-shrink","0"],[2,"color","rgba(184,228,249,0.6)","font-size","0.85rem","line-height","1.6"]],template:function(t,i){t&1&&(U(0,"footer",0)(1,"div",1)(2,"div",2)(3,"div")(4,"div",3)(5,"div",4),j(6,"\u{1F48A}"),z(),U(7,"div")(8,"div",5),j(9,"\u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628"),z(),U(10,"div",6),j(11,"\u0644\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629"),z()()(),U(12,"p",7),j(13," \u0646\u0648\u0641\u0631 \u0623\u0641\u0636\u0644 \u0627\u0644\u0623\u062F\u0648\u064A\u0629 \u0648\u0627\u0644\u0645\u0633\u062A\u0644\u0632\u0645\u0627\u062A \u0627\u0644\u0637\u0628\u064A\u0629 \u0628\u0623\u0639\u0644\u0649 \u0645\u0639\u0627\u064A\u064A\u0631 \u0627\u0644\u062C\u0648\u062F\u0629\u060C \u0645\u0644\u062A\u0632\u0645\u0648\u0646 \u0628\u0635\u062D\u062A\u0643 \u0648\u0633\u0644\u0627\u0645\u0629 \u0639\u0627\u0626\u0644\u062A\u0643 \u0645\u0646\u0630 \u0623\u0643\u062B\u0631 \u0645\u0646 20 \u0639\u0627\u0645\u0627\u064B. "),z(),U(14,"div",8),vt(15,KR,2,2,"button",9,YR),z()(),U(17,"div")(18,"h4",10),j(19,"\u0631\u0648\u0627\u0628\u0637 \u0633\u0631\u064A\u0639\u0629"),z(),U(20,"ul",11),vt(21,QR,5,2,"li",12,ZR),z()(),U(23,"div")(24,"h4",10),j(25,"\u062E\u062F\u0645\u0627\u062A\u0646\u0627"),z(),U(26,"ul",11),vt(27,eN,5,1,"li",12,as),z()(),U(29,"div")(30,"h4",10),j(31,"\u062A\u0648\u0627\u0635\u0644 \u0645\u0639\u0646\u0627"),z(),vt(32,tN,5,2,"div",13,JR),z()(),U(34,"div",14)(35,"p",15),j(36),z(),U(37,"div",16)(38,"a",17),j(39,"\u0633\u064A\u0627\u0633\u0629 \u0627\u0644\u062E\u0635\u0648\u0635\u064A\u0629"),z(),U(40,"a",17),j(41,"\u0634\u0631\u0648\u0637 \u0627\u0644\u0627\u0633\u062A\u062E\u062F\u0627\u0645"),z(),U(42,"a",17),j(43,"\u0633\u064A\u0627\u0633\u0629 \u0627\u0644\u0625\u0631\u062C\u0627\u0639"),z()()()()()),t&2&&(oe(15),yt(i.socialLinks),oe(6),yt(i.quickLinks),oe(6),yt(i.services),oe(5),yt(i.contactItems),oe(4),Nn("\xA9 ",i.currentYear," \u0627\u0644\u0623\u0645\u064A\u0646 \u0642\u0631\u0648\u0628 \u0644\u0644\u0623\u062F\u0648\u064A\u0629 \u2014 \u062C\u0645\u064A\u0639 \u0627\u0644\u062D\u0642\u0648\u0642 \u0645\u062D\u0641\u0648\u0638\u0629"))},encapsulation:2})};function oN(n,e){if(n&1){let t=vn();gr(0,"app-loading-screen",6),$i("onDone",function(){sn(t);let r=Tt();return on(r.handleLoadingDone())}),cs()}}var Zd=class n{loading=Yt(!0);handleLoadingDone(){this.loading.set(!1)}scrollTop(){window.scrollTo({top:0,behavior:"smooth"})}onBtnHover(e){let t=e.currentTarget;t.style.transform="translateY(-4px) scale(1.1)",t.style.boxShadow="0 10px 35px rgba(45,156,219,0.6)"}onBtnLeave(e){let t=e.currentTarget;t.style.transform="translateY(0) scale(1)",t.style.boxShadow="0 6px 24px rgba(45,156,219,0.45)"}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ct({type:n,selectors:[["app-root"]],decls:22,vars:1,consts:[["fromColor","#071525","toColor","#F0FAFF"],["fromColor","#F0FAFF","toColor","#E8F7FF"],["fromColor","#E8F7FF","toColor","#F0FAFF"],["fromColor","#F0FAFF","toColor","#071525"],["fromColor","#071525","toColor","#E8F7FF"],["title","\u0627\u0644\u0639\u0648\u062F\u0629 \u0644\u0644\u0623\u0639\u0644\u0649",2,"position","fixed","bottom","2rem","left","2rem","width","48px","height","48px","border-radius","50%","background","linear-gradient(135deg, #2D9CDB, #1a7ab5)","border","none","color","white","font-size","1.3rem","cursor","none","display","flex","align-items","center","justify-content","center","box-shadow","0 6px 24px rgba(45,156,219,0.45)","z-index","500","transition","transform 0.3s ease, box-shadow 0.3s ease",3,"click","mouseenter","mouseleave"],[3,"onDone"]],template:function(t,i){t&1&&(ls(0,"app-custom-cursor"),ci(1,oN,1,0,"app-loading-screen"),ls(2,"app-navbar"),gr(3,"main"),ls(4,"app-hero-section")(5,"app-stats-section")(6,"app-wave-separator",0)(7,"app-about-section")(8,"app-wave-separator",1)(9,"app-products-section")(10,"app-wave-separator",2)(11,"app-services-section")(12,"app-wave-separator",3)(13,"app-partners-section")(14,"app-wave-separator",4)(15,"app-testimonials-section")(16,"app-wave-separator",2)(17,"app-contact-section"),cs(),ls(18,"app-wave-separator",3)(19,"app-footer"),gr(20,"button",5),$i("click",function(){return i.scrollTop()})("mouseenter",function(s){return i.onBtnHover(s)})("mouseleave",function(s){return i.onBtnLeave(s)}),j(21,"\u2191"),cs()),t&2&&(oe(),li(i.loading()?1:-1))},dependencies:[Fl,kl,Ul,Bd,Vd,Hd,zd,Wd,jd,$d,qd,Xd,Yd],encapsulation:2})};Bp(Zd,x_).catch(n=>console.error(n));
