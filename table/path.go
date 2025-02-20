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
	"fmt"
	log "github.com/Sirupsen/logrus"
	api "github.com/osrg/gobgp/api"
	"github.com/osrg/gobgp/config"
	"github.com/osrg/gobgp/packet"
	"math"
	"net"
	"time"
)

type Bitmap []uint64

func (b Bitmap) Flag(i uint) {
	b[i/64] |= 1 << uint(i%64)
}

func (b Bitmap) Unflag(i uint) {
	b[i/64] &^= 1 << uint(i%64)
}

func (b Bitmap) GetFlag(i uint) bool {
	return b[i/64]&(1<<uint(i%64)) > 0
}

func NewBitmap(size int) Bitmap {
	return Bitmap(make([]uint64, (size+64-1)/64))
}

type originInfo struct {
	nlri               bgp.AddrPrefixInterface
	source             *PeerInfo
	timestamp          time.Time
	noImplicitWithdraw bool
	validation         config.RpkiValidationResultType
	isFromZebra        bool
	key                string
	uuid               []byte
}

type Path struct {
	info       *originInfo
	IsWithdraw bool
	pathAttrs  []bgp.PathAttributeInterface
	reason     BestPathReason
	parent     *Path
	dels       []bgp.BGPAttrType
	filtered   map[string]PolicyDirection
}

func NewPath(source *PeerInfo, nlri bgp.AddrPrefixInterface, isWithdraw bool, pattrs []bgp.PathAttributeInterface, timestamp time.Time, noImplicitWithdraw bool) *Path {
	if !isWithdraw && pattrs == nil {
		log.WithFields(log.Fields{
			"Topic": "Table",
			"Key":   nlri.String(),
			"Peer":  source.Address.String(),
		}).Error("Need to provide patattrs for the path that is not withdraw.")
		return nil
	}

	return &Path{
		info: &originInfo{
			nlri:               nlri,
			source:             source,
			timestamp:          timestamp,
			noImplicitWithdraw: noImplicitWithdraw,
		},
		IsWithdraw: isWithdraw,
		pathAttrs:  pattrs,
		filtered:   make(map[string]PolicyDirection),
	}
}

func cloneAsPath(asAttr *bgp.PathAttributeAsPath) *bgp.PathAttributeAsPath {
	newASparams := make([]bgp.AsPathParamInterface, len(asAttr.Value))
	for i, param := range asAttr.Value {
		asParam := param.(*bgp.As4PathParam)
		as := make([]uint32, len(asParam.AS))
		copy(as, asParam.AS)
		newASparams[i] = bgp.NewAs4PathParam(asParam.Type, as)
	}
	return bgp.NewPathAttributeAsPath(newASparams)
}

func (path *Path) UpdatePathAttrs(global *config.Global, peer *config.Neighbor) {

	if peer.RouteServer.Config.RouteServerClient {
		return
	}

	localAddress := net.ParseIP(peer.Transport.Config.LocalAddress)
	if peer.Config.PeerType == config.PEER_TYPE_EXTERNAL {
		// NEXTHOP handling
		path.SetNexthop(localAddress)

		// AS_PATH handling
		path.PrependAsn(global.Config.As, 1)

		// MED Handling
		if med := path.getPathAttr(bgp.BGP_ATTR_TYPE_MULTI_EXIT_DISC); med != nil && !path.IsLocal() {
			path.delPathAttr(bgp.BGP_ATTR_TYPE_MULTI_EXIT_DISC)
		}

		// remove local-pref attribute
		if pref := path.getPathAttr(bgp.BGP_ATTR_TYPE_LOCAL_PREF); pref != nil && !config.IsConfederationMember(global, peer) {
			path.delPathAttr(bgp.BGP_ATTR_TYPE_LOCAL_PREF)
		}

	} else if peer.Config.PeerType == config.PEER_TYPE_INTERNAL {
		// NEXTHOP handling for iBGP
		// if the path generated locally set local address as nexthop.
		// if not, don't modify it.
		// TODO: NEXT-HOP-SELF support
		nexthop := path.GetNexthop()
		if path.IsLocal() && (nexthop.Equal(net.ParseIP("0.0.0.0")) || nexthop.Equal(net.ParseIP("::"))) {
			path.SetNexthop(localAddress)
		}

		// AS_PATH handling for iBGP
		// if the path has AS_PATH path attribute, don't modify it.
		// if not, attach *empty* AS_PATH path attribute.
		if nh := path.getPathAttr(bgp.BGP_ATTR_TYPE_AS_PATH); nh == nil {
			path.PrependAsn(0, 0)
		}

		// For iBGP peers we are required to send local-pref attribute
		// for connected or local prefixes.
		// We set default local-pref 100.
		if pref := path.getPathAttr(bgp.BGP_ATTR_TYPE_LOCAL_PREF); pref == nil || !path.IsLocal() {
			path.setPathAttr(bgp.NewPathAttributeLocalPref(100))
		}

		// RFC4456: BGP Route Reflection
		// 8. Avoiding Routing Information Loops
		info := path.GetSource()
		if peer.RouteReflector.Config.RouteReflectorClient {
			// This attribute will carry the BGP Identifier of the originator of the route in the local AS.
			// A BGP speaker SHOULD NOT create an ORIGINATOR_ID attribute if one already exists.
			if path.getPathAttr(bgp.BGP_ATTR_TYPE_ORIGINATOR_ID) == nil {
				path.setPathAttr(bgp.NewPathAttributeOriginatorId(info.ID.String()))
			}
			// When an RR reflects a route, it MUST prepend the local CLUSTER_ID to the CLUSTER_LIST.
			// If the CLUSTER_LIST is empty, it MUST create a new one.
			id := string(peer.RouteReflector.Config.RouteReflectorClusterId)
			if p := path.getPathAttr(bgp.BGP_ATTR_TYPE_CLUSTER_LIST); p == nil {
				path.setPathAttr(bgp.NewPathAttributeClusterList([]string{id}))
			} else {
				clusterList := p.(*bgp.PathAttributeClusterList)
				newClusterList := make([]string, 0, len(clusterList.Value))
				for _, ip := range clusterList.Value {
					newClusterList = append(newClusterList, ip.String())
				}
				path.setPathAttr(bgp.NewPathAttributeClusterList(append([]string{id}, newClusterList...)))
			}
		}

	} else {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.Config.NeighborAddress,
		}).Warnf("invalid peer type: %d", peer.Config.PeerType)
	}
}

func (path *Path) GetTimestamp() time.Time {
	return path.OriginInfo().timestamp
}

func (path *Path) setTimestamp(t time.Time) {
	path.OriginInfo().timestamp = t
}

func (path *Path) IsLocal() bool {
	return path.GetSource().Address == nil
}

func (path *Path) IsIBGP() bool {
	return path.GetSource().AS == path.GetSource().LocalAS
}

func (path *Path) ToApiStruct(id string) *api.Path {
	nlri := path.GetNlri()
	n, _ := nlri.Serialize()
	family := uint32(bgp.AfiSafiToRouteFamily(nlri.AFI(), nlri.SAFI()))
	pattrs := func(arg []bgp.PathAttributeInterface) [][]byte {
		ret := make([][]byte, 0, len(arg))
		for _, a := range arg {
			aa, _ := a.Serialize()
			ret = append(ret, aa)
		}
		return ret
	}(path.GetPathAttrs())
	return &api.Path{
		Nlri:       n,
		Pattrs:     pattrs,
		Age:        int64(time.Now().Sub(path.OriginInfo().timestamp).Seconds()),
		IsWithdraw: path.IsWithdraw,
		Validation: int32(path.OriginInfo().validation.ToInt()),
		Filtered:   path.Filtered(id) == POLICY_DIRECTION_IN,
		Family:     family,
		SourceAsn:  path.OriginInfo().source.AS,
		SourceId:   path.OriginInfo().source.ID.String(),
	}
}

// create new PathAttributes
func (path *Path) Clone(isWithdraw bool) *Path {
	return &Path{
		parent:     path,
		IsWithdraw: isWithdraw,
		filtered:   make(map[string]PolicyDirection),
	}
}

func (path *Path) root() *Path {
	p := path
	for p.parent != nil {
		p = p.parent
	}
	return p
}

func (path *Path) OriginInfo() *originInfo {
	return path.root().info
}

func (path *Path) NoImplicitWithdraw() bool {
	return path.OriginInfo().noImplicitWithdraw
}

func (path *Path) Validation() config.RpkiValidationResultType {
	return path.OriginInfo().validation
}

func (path *Path) SetValidation(r config.RpkiValidationResultType) {
	path.OriginInfo().validation = r
}

func (path *Path) IsFromZebra() bool {
	return path.OriginInfo().isFromZebra
}

func (path *Path) SetIsFromZebra(y bool) {
	path.OriginInfo().isFromZebra = y
}

func (path *Path) UUID() []byte {
	return path.OriginInfo().uuid
}

func (path *Path) SetUUID(uuid []byte) {
	path.OriginInfo().uuid = uuid
}

func (path *Path) Filter(id string, reason PolicyDirection) {
	path.filtered[id] = reason
}

func (path *Path) Filtered(id string) PolicyDirection {
	return path.filtered[id]
}

func (path *Path) GetRouteFamily() bgp.RouteFamily {
	return bgp.AfiSafiToRouteFamily(path.OriginInfo().nlri.AFI(), path.OriginInfo().nlri.SAFI())
}

func (path *Path) setSource(source *PeerInfo) {
	path.OriginInfo().source = source
}
func (path *Path) GetSource() *PeerInfo {
	return path.OriginInfo().source
}

func (path *Path) GetSourceAs() uint32 {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_AS_PATH)
	if attr != nil {
		asPathParam := attr.(*bgp.PathAttributeAsPath).Value
		if len(asPathParam) == 0 {
			return 0
		}
		asPath := asPathParam[len(asPathParam)-1].(*bgp.As4PathParam)
		if asPath.Num == 0 {
			return 0
		}
		return asPath.AS[asPath.Num-1]
	}
	return 0
}

func (path *Path) GetNexthop() net.IP {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_NEXT_HOP)
	if attr != nil {
		return attr.(*bgp.PathAttributeNextHop).Value
	}
	attr = path.getPathAttr(bgp.BGP_ATTR_TYPE_MP_REACH_NLRI)
	if attr != nil {
		return attr.(*bgp.PathAttributeMpReachNLRI).Nexthop
	}
	return net.IP{}
}

func (path *Path) SetNexthop(nexthop net.IP) {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_NEXT_HOP)
	if attr != nil {
		path.setPathAttr(bgp.NewPathAttributeNextHop(nexthop.String()))
	}
	attr = path.getPathAttr(bgp.BGP_ATTR_TYPE_MP_REACH_NLRI)
	if attr != nil {
		oldNlri := attr.(*bgp.PathAttributeMpReachNLRI)
		path.setPathAttr(bgp.NewPathAttributeMpReachNLRI(nexthop.String(), oldNlri.Value))
	}
}

func (path *Path) GetNlri() bgp.AddrPrefixInterface {
	return path.OriginInfo().nlri
}

func (path *Path) GetPathAttrs() []bgp.PathAttributeInterface {
	seen := NewBitmap(math.MaxUint8)
	list := make([]bgp.PathAttributeInterface, 0, 4)
	p := path
	for {
		for _, t := range p.dels {
			seen.Flag(uint(t))
		}
		for _, a := range p.pathAttrs {
			if typ := uint(a.GetType()); !seen.GetFlag(typ) {
				list = append(list, a)
				seen.Flag(typ)
			}
		}
		if p.parent == nil {
			return list
		}
		p = p.parent
	}
}

func (path *Path) getPathAttr(typ bgp.BGPAttrType) bgp.PathAttributeInterface {
	p := path
	for {
		for _, t := range p.dels {
			if t == typ {
				return nil
			}
		}
		for _, a := range p.pathAttrs {
			if a.GetType() == typ {
				return a
			}
		}
		if p.parent == nil {
			return nil
		}
		p = p.parent
	}
}

func (path *Path) setPathAttr(a bgp.PathAttributeInterface) {
	if len(path.pathAttrs) == 0 {
		path.pathAttrs = []bgp.PathAttributeInterface{a}
	} else {
		for i, b := range path.pathAttrs {
			if a.GetType() == b.GetType() {
				path.pathAttrs[i] = a
				return
			}
		}
		path.pathAttrs = append(path.pathAttrs, a)
	}
}

func (path *Path) delPathAttr(typ bgp.BGPAttrType) {
	if len(path.dels) == 0 {
		path.dels = []bgp.BGPAttrType{typ}
	} else {
		path.dels = append(path.dels, typ)
	}
}

// return Path's string representation
func (path *Path) String() string {
	s := bytes.NewBuffer(make([]byte, 0, 64))
	s.WriteString(fmt.Sprintf("{ %s | ", path.getPrefix()))
	s.WriteString(fmt.Sprintf("src: %s", path.GetSource()))
	s.WriteString(fmt.Sprintf(", nh: %s", path.GetNexthop()))
	if path.IsWithdraw {
		s.WriteString(", withdraw")
	}
	s.WriteString(" }")
	return s.String()
}

func (path *Path) getPrefix() string {
	if path.OriginInfo().key == "" {
		path.OriginInfo().key = path.GetNlri().String()
	}
	return path.OriginInfo().key
}

func (path *Path) GetAsPath() *bgp.PathAttributeAsPath {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_AS_PATH)
	if attr != nil {
		return attr.(*bgp.PathAttributeAsPath)
	}
	return nil
}

// GetAsPathLen returns the number of AS_PATH
func (path *Path) GetAsPathLen() int {

	var length int = 0
	if aspath := path.GetAsPath(); aspath != nil {
		for _, as := range aspath.Value {
			length += as.ASLen()
		}
	}
	return length
}

func (path *Path) GetAsString() string {
	s := bytes.NewBuffer(make([]byte, 0, 64))
	if aspath := path.GetAsPath(); aspath != nil {
		for i, paramIf := range aspath.Value {
			segment := paramIf.(*bgp.As4PathParam)
			if i != 0 {
				s.WriteString(" ")
			}

			sep := " "
			switch segment.Type {
			case bgp.BGP_ASPATH_ATTR_TYPE_SET, bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET:
				s.WriteString("{")
				sep = ","
			}
			for j, as := range segment.AS {
				s.WriteString(fmt.Sprintf("%d", as))
				if j != len(segment.AS)-1 {
					s.WriteString(sep)
				}
			}
			switch segment.Type {
			case bgp.BGP_ASPATH_ATTR_TYPE_SET, bgp.BGP_ASPATH_ATTR_TYPE_CONFED_SET:
				s.WriteString("}")
			}
		}
	}
	return s.String()
}

func (path *Path) GetAsList() []uint32 {
	return path.getAsListofSpecificType(true, true)

}

func (path *Path) GetAsSeqList() []uint32 {
	return path.getAsListofSpecificType(true, false)

}

func (path *Path) getAsListofSpecificType(getAsSeq, getAsSet bool) []uint32 {
	asList := []uint32{}
	if aspath := path.GetAsPath(); aspath != nil {
		for _, paramIf := range aspath.Value {
			segment := paramIf.(*bgp.As4PathParam)
			if getAsSeq && segment.Type == bgp.BGP_ASPATH_ATTR_TYPE_SEQ {
				asList = append(asList, segment.AS...)
				continue
			}
			if getAsSet && segment.Type == bgp.BGP_ASPATH_ATTR_TYPE_SET {
				asList = append(asList, segment.AS...)
			} else {
				asList = append(asList, 0)
			}
		}
	}
	return asList
}

// PrependAsn prepends AS number.
// This function updates the AS_PATH attribute as follows.
//  1) if the first path segment of the AS_PATH is of type
//     AS_SEQUENCE, the local system prepends the specified AS num as
//     the last element of the sequence (put it in the left-most
//     position with respect to the position of  octets in the
//     protocol message) the specified number of times.
//     If the act of prepending will cause an overflow in the AS_PATH
//     segment (i.e.,  more than 255 ASes),
//     it SHOULD prepend a new segment of type AS_SEQUENCE
//     and prepend its own AS number to this new segment.
//
//  2) if the first path segment of the AS_PATH is of other than type
//     AS_SEQUENCE, the local system prepends a new path segment of type
//     AS_SEQUENCE to the AS_PATH, including the specified AS number in
//     that segment.
//
//  3) if the AS_PATH is empty, the local system creates a path
//     segment of type AS_SEQUENCE, places the specified AS number
//     into that segment, and places that segment into the AS_PATH.
func (path *Path) PrependAsn(asn uint32, repeat uint8) {

	original := path.GetAsPath()

	asns := make([]uint32, repeat)
	for i, _ := range asns {
		asns[i] = asn
	}

	var asPath *bgp.PathAttributeAsPath
	if original == nil {
		asPath = bgp.NewPathAttributeAsPath([]bgp.AsPathParamInterface{})
	} else {
		asPath = cloneAsPath(original)
	}

	if len(asPath.Value) > 0 {
		fst := asPath.Value[0].(*bgp.As4PathParam)
		if fst.Type == bgp.BGP_ASPATH_ATTR_TYPE_SEQ {
			if len(fst.AS)+int(repeat) > 255 {
				repeat = uint8(255 - len(fst.AS))
			}
			fst.AS = append(asns[:int(repeat)], fst.AS...)
			fst.Num += repeat
			asns = asns[int(repeat):]
		}
	}

	if len(asns) > 0 {
		p := bgp.NewAs4PathParam(bgp.BGP_ASPATH_ATTR_TYPE_SEQ, asns)
		asPath.Value = append([]bgp.AsPathParamInterface{p}, asPath.Value...)
	}
	path.setPathAttr(asPath)
}

func (path *Path) GetCommunities() []uint32 {
	communityList := []uint32{}
	if attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_COMMUNITIES); attr != nil {
		communities := attr.(*bgp.PathAttributeCommunities)
		communityList = append(communityList, communities.Value...)
	}
	return communityList
}

// SetCommunities adds or replaces communities with new ones.
// If the length of communities is 0 and doReplace is true, it clears communities.
func (path *Path) SetCommunities(communities []uint32, doReplace bool) {

	if len(communities) == 0 && doReplace {
		// clear communities
		path.delPathAttr(bgp.BGP_ATTR_TYPE_COMMUNITIES)
		return
	}

	newList := make([]uint32, 0)
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_COMMUNITIES)
	if attr != nil {
		c := attr.(*bgp.PathAttributeCommunities)
		if doReplace {
			newList = append(newList, communities...)
		} else {
			newList = append(newList, c.Value...)
			newList = append(newList, communities...)
		}
	} else {
		newList = append(newList, communities...)
	}
	path.setPathAttr(bgp.NewPathAttributeCommunities(newList))

}

// RemoveCommunities removes specific communities.
// If the length of communites is 0, it does nothing.
// If all communities are removed, it removes Communities path attribute itself.
func (path *Path) RemoveCommunities(communities []uint32) int {

	if len(communities) == 0 {
		// do nothing
		return 0
	}

	find := func(val uint32) bool {
		for _, com := range communities {
			if com == val {
				return true
			}
		}
		return false
	}

	count := 0
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_COMMUNITIES)
	if attr != nil {
		newList := make([]uint32, 0)
		c := attr.(*bgp.PathAttributeCommunities)

		for _, value := range c.Value {
			if find(value) {
				count += 1
			} else {
				newList = append(newList, value)
			}
		}

		if len(newList) != 0 {
			path.setPathAttr(bgp.NewPathAttributeCommunities(newList))
		} else {
			path.delPathAttr(bgp.BGP_ATTR_TYPE_COMMUNITIES)
		}
	}
	return count
}

func (path *Path) GetExtCommunities() []bgp.ExtendedCommunityInterface {
	eCommunityList := make([]bgp.ExtendedCommunityInterface, 0)
	if attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_EXTENDED_COMMUNITIES); attr != nil {
		eCommunities := attr.(*bgp.PathAttributeExtendedCommunities).Value
		for _, eCommunity := range eCommunities {
			eCommunityList = append(eCommunityList, eCommunity)
		}
	}
	return eCommunityList
}

func (path *Path) SetExtCommunities(exts []bgp.ExtendedCommunityInterface, doReplace bool) {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_EXTENDED_COMMUNITIES)
	if attr != nil {
		l := attr.(*bgp.PathAttributeExtendedCommunities).Value
		if doReplace {
			l = exts
		} else {
			l = append(l, exts...)
		}
		path.setPathAttr(bgp.NewPathAttributeExtendedCommunities(l))
	} else {
		path.setPathAttr(bgp.NewPathAttributeExtendedCommunities(exts))
	}
}

func (path *Path) GetMed() (uint32, error) {
	attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_MULTI_EXIT_DISC)
	if attr == nil {
		return 0, fmt.Errorf("no med path attr")
	}
	return attr.(*bgp.PathAttributeMultiExitDisc).Value, nil
}

// SetMed replace, add or subtraction med with new ones.
func (path *Path) SetMed(med int64, doReplace bool) error {

	parseMed := func(orgMed uint32, med int64, doReplace bool) (*bgp.PathAttributeMultiExitDisc, error) {
		newMed := &bgp.PathAttributeMultiExitDisc{}
		if doReplace {
			newMed = bgp.NewPathAttributeMultiExitDisc(uint32(med))
		} else {
			if int64(orgMed)+med < 0 {
				return nil, fmt.Errorf("med value invalid. it's underflow threshold.")
			} else if int64(orgMed)+med > int64(math.MaxUint32) {
				return nil, fmt.Errorf("med value invalid. it's overflow threshold.")
			}
			newMed = bgp.NewPathAttributeMultiExitDisc(uint32(int64(orgMed) + med))
		}
		return newMed, nil
	}

	m := uint32(0)
	if attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_MULTI_EXIT_DISC); attr != nil {
		m = attr.(*bgp.PathAttributeMultiExitDisc).Value
	}
	newMed, err := parseMed(m, med, doReplace)
	if err != nil {
		return err
	}
	path.setPathAttr(newMed)
	return nil
}

func (path *Path) GetOriginatorID() net.IP {
	if attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_ORIGINATOR_ID); attr != nil {
		return attr.(*bgp.PathAttributeOriginatorId).Value
	}
	return nil
}

func (path *Path) GetClusterList() []net.IP {
	if attr := path.getPathAttr(bgp.BGP_ATTR_TYPE_CLUSTER_LIST); attr != nil {
		return attr.(*bgp.PathAttributeClusterList).Value
	}
	return nil
}

func (lhs *Path) Equal(rhs *Path) bool {
	return lhs == rhs
}
