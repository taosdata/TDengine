package prompb

import (
	"fmt"
	"io"
)

var (
	ErrInvalidLengthRemote        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRemote          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupRemote = fmt.Errorf("proto: unexpected end of group")
)

type WriteRequest struct {
	Timeseries  []TimeSeries `protobuf:"bytes,1,rep,name=timeseries,proto3" json:"timeseries,omitempty"`
	labelsPool  []Label
	samplesPool []Sample
}

func (m *WriteRequest) Reset() {
	for i := 0; i < len(m.Timeseries); i++ {
		ts := &m.Timeseries[i]
		ts.Labels = nil
		ts.Samples = nil
	}
	m.Timeseries = m.Timeseries[:0]

	for i := 0; i < len(m.labelsPool); i++ {
		l := &m.labelsPool[i]
		l.Name = nil
		l.Value = nil
	}
	m.labelsPool = m.labelsPool[:0]
	for i := 0; i < len(m.samplesPool); i++ {
		s := &m.samplesPool[i]
		s.Value = 0
		s.Timestamp = 0
	}
	m.samplesPool = m.samplesPool[:0]
}

func (m *WriteRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRemote
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: WriteRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: WriteRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRemote
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRemote
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if cap(m.Timeseries) > len(m.Timeseries) {
				m.Timeseries = m.Timeseries[:len(m.Timeseries)+1]
			} else {
				m.Timeseries = append(m.Timeseries, TimeSeries{})
			}
			ts := &m.Timeseries[len(m.Timeseries)-1]
			var err error
			m.labelsPool, m.samplesPool, err = ts.Unmarshal(dAtA[iNdEx:postIndex], m.labelsPool, m.samplesPool)
			if err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRemote(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRemote
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func skipRemote(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRemote
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRemote
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthRemote
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupRemote
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthRemote
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}
