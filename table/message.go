// Copyright (C) 2014 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	log "github.com/Sirupsen/logrus"
	"github.com/osrg/gobgp/packet"
	"hash/fnv"
)

func UpdatePathAttrs2ByteAs(msg *bgp.BGPUpdate) error {
	ps := msg.PathAttributes
	msg.PathAttributes = make([]bgp.PathAttributeInterface, len(ps))
	copy(msg.PathAttributes, ps)
	var asAttr *bgp.PathAttributeAsPath
	idx := 0
	for i, attr := range msg.PathAttributes {
		if a, ok := attr.(*bgp.PathAttributeAsPath); ok {
			asAttr = a
			idx = i
			break
		}
	}

	if asAttr == nil {
		return nil
	}

	as4Params := make([]*bgp.As4PathParam, 0, len(asAttr.Value))
	as2Params := make([]bgp.AsPathParamInterface, 0, len(asAttr.Value))
	mkAs4 := false
	for _, param := range asAttr.Value {
		as4Param := param.(*bgp.As4PathParam)
		as2Path := make([]uint16, 0, len(as4Param.AS))
		for _, as := range as4Param.AS {
			if as > (1<<16)-1 {
				mkAs4 = true
				as2Path = append(as2Path, bgp.AS_TRANS)
			} else {
				as2Path = append(as2Path, uint16(as))
			}
		}
		as2Params = append(as2Params, bgp.NewAsPathParam(as4Param.Type, as2Path))

		// RFC 6793 4.2.2 Generating Updates
		//
		// Whenever the AS path information contains the AS_CONFED_SEQUENCE or
		// AS_CONFED_SET path segment, the NEW BGP speaker MUST exclude such
		// path segments from the AS4_PATH attribute being constructed.
		if as4Param.Type != bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SEQ && as4Param.Type != bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET {
			as4Params = append(as4Params, as4Param)
		}
	}
	msg.PathAttributes[idx] = bgp.NewPathAttributeAsPath(as2Params)
	if mkAs4 {
		msg.PathAttributes = append(msg.PathAttributes, bgp.NewPathAttributeAs4Path(as4Params))
	}
	return nil
}

func UpdatePathAttrs4ByteAs(msg *bgp.BGPUpdate) error {
	var asAttr *bgp.PathAttributeAsPath
	var as4Attr *bgp.PathAttributeAs4Path
	asAttrPos := 0
	as4AttrPos := 0
	for i, attr := range msg.PathAttributes {
		switch attr.(type) {
		case *bgp.PathAttributeAsPath:
			asAttr = attr.(*bgp.PathAttributeAsPath)
			for j, param := range asAttr.Value {
				as2Param, ok := param.(*bgp.AsPathParam)
				if ok {
					asPath := make([]uint32, 0, len(as2Param.AS))
					for _, as := range as2Param.AS {
						asPath = append(asPath, uint32(as))
					}
					as4Param := bgp.NewAs4PathParam(as2Param.Type, asPath)
					asAttr.Value[j] = as4Param
				}
			}
			asAttrPos = i
			msg.PathAttributes[i] = asAttr
		case *bgp.PathAttributeAs4Path:
			as4AttrPos = i
			as4Attr = attr.(*bgp.PathAttributeAs4Path)
		}
	}

	if as4Attr != nil {
		msg.PathAttributes = append(msg.PathAttributes[:as4AttrPos], msg.PathAttributes[as4AttrPos+1:]...)
	}

	if asAttr == nil || as4Attr == nil {
		return nil
	}

	asLen := 0
	asConfedLen := 0
	asParams := make([]*bgp.As4PathParam, 0, len(asAttr.Value))
	for _, param := range asAttr.Value {
		asLen += param.ASLen()
		p := param.(*bgp.As4PathParam)
		switch p.Type {
		case bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET:
			asConfedLen += 1
		case bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SEQ:
			asConfedLen += len(p.AS)
		}
		asParams = append(asParams, p)
	}

	as4Len := 0
	as4Params := make([]*bgp.As4PathParam, 0, len(as4Attr.Value))
	if as4Attr != nil {
		for _, p := range as4Attr.Value {
			// RFC 6793 6. Error Handling
			//
			// the path segment types AS_CONFED_SEQUENCE and AS_CONFED_SET [RFC5065]
			// MUST NOT be carried in the AS4_PATH attribute of an UPDATE message.
			// A NEW BGP speaker that receives these path segment types in the AS4_PATH
			// attribute of an UPDATE message from an OLD BGP speaker MUST discard
			// these path segments, adjust the relevant attribute fields accordingly,
			// and continue processing the UPDATE message.
			// This case SHOULD be logged locally for analysis.
			switch p.Type {
			case bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SEQ, bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET:
				typ := "CONFED_SEQ"
				if p.Type == bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET {
					typ = "CONFED_SET"
				}
				log.Warnf("AS4_PATH contains %s segment %s. ignore", typ, p.String())
				continue
			}
			as4Len += p.ASLen()
			as4Params = append(as4Params, p)
		}
	}

	if asLen+asConfedLen < as4Len {
		log.Warnf("AS4_PATH is longer than AS_PATH. ignore AS4_PATH")
		return nil
	}

	keepNum := asLen + asConfedLen - as4Len

	newParams := make([]*bgp.As4PathParam, 0, len(asAttr.Value))
	for _, param := range asParams {
		if keepNum-param.ASLen() >= 0 {
			newParams = append(newParams, param)
			keepNum -= param.ASLen()
		} else {
			// only SEQ param reaches here
			param.AS = param.AS[:keepNum]
			newParams = append(newParams, param)
			keepNum = 0
		}

		if keepNum <= 0 {
			break
		}
	}

	for _, param := range as4Params {
		lastParam := newParams[len(newParams)-1]
		if param.Type == lastParam.Type && param.Type == bgp.BGP_ASPATH_ATTR_TYPE_SEQ {
			if len(lastParam.AS)+len(param.AS) > 255 {
				lastParam.AS = append(lastParam.AS, param.AS[:255-len(lastParam.AS)]...)
				param.AS = param.AS[255-len(lastParam.AS):]
				newParams = append(newParams, param)
			} else {
				lastParam.AS = append(lastParam.AS, param.AS...)
			}
		} else {
			newParams = append(newParams, param)
		}
	}

	newIntfParams := make([]bgp.AsPathParamInterface, 0, len(asAttr.Value))
	for _, p := range newParams {
		newIntfParams = append(newIntfParams, p)
	}

	msg.PathAttributes[asAttrPos] = bgp.NewPathAttributeAsPath(newIntfParams)
	return nil
}

func createUpdateMsgFromPath(path *Path, msg *bgp.BGPMessage) *bgp.BGPMessage {
	rf := path.GetRouteFamily()

	if rf == bgp.RF_IPv4_UC {
		nlri := path.GetNlri().(*bgp.IPAddrPrefix)
		if path.IsWithdraw {
			if msg != nil {
				u := msg.Body.(*bgp.BGPUpdate)
				u.WithdrawnRoutes = append(u.WithdrawnRoutes, nlri)
				return nil
			} else {
				return bgp.NewBGPUpdateMessage([]*bgp.IPAddrPrefix{nlri}, nil, nil)
			}
		} else {
			if msg != nil {
				u := msg.Body.(*bgp.BGPUpdate)
				u.NLRI = append(u.NLRI, nlri)
			} else {
				pathAttrs := path.GetPathAttrs()
				return bgp.NewBGPUpdateMessage(nil, pathAttrs, []*bgp.IPAddrPrefix{nlri})
			}
		}
	} else {
		if path.IsWithdraw {
			if msg != nil {
				u := msg.Body.(*bgp.BGPUpdate)
				for _, p := range u.PathAttributes {
					if p.GetType() == bgp.BGP_ATTR_TYPE_MP_UNREACH_NLRI {
						unreach := p.(*bgp.PathAttributeMpUnreachNLRI)
						unreach.Value = append(unreach.Value, path.GetNlri())
					}
				}
			} else {
				var nlris []bgp.AddrPrefixInterface
				attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_MP_REACH_NLRI)
				if attr == nil {
					// for bmp post-policy
					attr = path.getPathAttr(bgp.BGP_ATTR_TYPE_MP_UNREACH_NLRI)
					nlris = attr.(*bgp.PathAttributeMpUnreachNLRI).Value
				} else {
					nlris = attr.(*bgp.PathAttributeMpReachNLRI).Value
				}

				clonedAttrs := path.GetPathAttrs()
				for i, a := range clonedAttrs {
					if a.GetType() == bgp.BGP_ATTR_TYPE_MP_UNREACH_NLRI || a.GetType() == bgp.BGP_ATTR_TYPE_MP_REACH_NLRI {
						clonedAttrs[i] = bgp.NewPathAttributeMpUnreachNLRI(nlris)
						break
					}
				}
				return bgp.NewBGPUpdateMessage(nil, clonedAttrs, nil)
			}
		} else {
			if msg != nil {
				u := msg.Body.(*bgp.BGPUpdate)
				for _, p := range u.PathAttributes {
					if p.GetType() == bgp.BGP_ATTR_TYPE_MP_REACH_NLRI {
						reach := p.(*bgp.PathAttributeMpReachNLRI)
						reach.Value = append(reach.Value, path.GetNlri())
					}
				}
			} else {
				// we don't need to clone here but we
				// might merge path to this message in
				// the future so let's clone anyway.
				return bgp.NewBGPUpdateMessage(nil, path.GetPathAttrs(), nil)
			}
		}
	}
	return nil
}

type bucket struct {
	attrs []byte
	paths []*Path
}

func CreateUpdateMsgFromPaths(pathList []*Path) []*bgp.BGPMessage {
	var msgs []*bgp.BGPMessage

	pathByAttrs := make(map[uint32][]*bucket)
	for _, path := range pathList {
		if path == nil {
			continue
		}
		y := func(p *Path) bool {
			if p.GetRouteFamily() != bgp.RF_IPv4_UC {
				return false
			}
			if p.IsWithdraw {
				return false
			}
			return true
		}(path)

		if y {
			key, attrs := func(p *Path) (uint32, []byte) {
				h := fnv.New32()
				total := bytes.NewBuffer(make([]byte, 0))
				for _, v := range p.GetPathAttrs() {
					b, _ := v.Serialize()
					total.Write(b)
				}
				h.Write(total.Bytes())
				return h.Sum32(), total.Bytes()
			}(path)

			if bl, y := pathByAttrs[key]; y {
				found := false
				for _, b := range bl {
					if bytes.Compare(b.attrs, attrs) == 0 {
						b.paths = append(b.paths, path)
						found = true
						break
					}
				}
				if found == false {
					nb := &bucket{
						attrs: attrs,
						paths: []*Path{path},
					}
					pathByAttrs[key] = append(pathByAttrs[key], nb)
				}
			} else {
				nb := &bucket{
					attrs: attrs,
					paths: []*Path{path},
				}
				pathByAttrs[key] = []*bucket{nb}
			}
		} else {
			msg := createUpdateMsgFromPath(path, nil)
			msgs = append(msgs, msg)
		}
	}

	for _, bList := range pathByAttrs {
		for _, b := range bList {
			var msg *bgp.BGPMessage
			for i, path := range b.paths {
				if i == 0 {
					msg = createUpdateMsgFromPath(path, nil)
					msgs = append(msgs, msg)
				} else {
					msgLen := func(u *bgp.BGPUpdate) int {
						attrsLen := 0
						for _, a := range u.PathAttributes {
							attrsLen += a.Len()
						}
						// Header + Update (WithdrawnRoutesLen +
						// TotalPathAttributeLen + attributes + maxlen of
						// NLRI). Note that we try to add one NLRI.
						return 19 + 2 + 2 + attrsLen + (len(u.NLRI)+1)*5
					}(msg.Body.(*bgp.BGPUpdate))

					if msgLen+32 > bgp.BGP_MAX_MESSAGE_LENGTH {
						// don't marge
						msg = createUpdateMsgFromPath(path, nil)
						msgs = append(msgs, msg)
					} else {
						createUpdateMsgFromPath(path, msg)
					}
				}
			}
		}
	}

	return msgs
}
