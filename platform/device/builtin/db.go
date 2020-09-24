package builtin

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"

	"github.com/micromdm/micromdm/platform/device"
)

const (
	DeviceBucket = "mdm.Devices"

	// The deviceIndexBucket index bucket stores serial number and UDID references
	// to the device uuid.
	deviceIndexBucket = "mdm.DeviceIdx"

	// The udidCertAuthBucket stores a simple mapping from UDID to
	// sha256 hash of the device identity certificate for future validation
	udidCertAuthBucket = "mdm.UDIDCertAuth"

	// Stores last seen for a device UDID
	deviceLastSeenBucket = "mdm.DeviceLastSeen"
)

type DB struct {
	*bolt.DB
	lastSeenDB *bolt.DB
}

func NewDB(db, lastdb *bolt.DB) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(deviceIndexBucket))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(DeviceBucket))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(udidCertAuthBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", DeviceBucket)
	}
	err = lastdb.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(deviceLastSeenBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", deviceLastSeenBucket)
	}
	datastore := &DB{DB: db, lastSeenDB: lastdb}
	return datastore, nil
}

func (db *DB) List(ctx context.Context, opt device.ListDevicesOption) ([]device.Device, error) {
	var devices []device.Device
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DeviceBucket))
		// TODO optimize by implemting Seek() and bytes.HasPrefix() so we don't
		// hit all keys in the database if we dont have to.
		return b.ForEach(func(k, v []byte) error {
			var dev device.Device
			if err := device.UnmarshalDevice(v, &dev); err != nil {
				return err
			}
			if len(opt.FilterSerial) == 0 {
				devices = append(devices, dev)
				return nil
			}
			for _, fs := range opt.FilterSerial {
				if fs == dev.SerialNumber {
					devices = append(devices, dev)
				}
			}
			return nil
		})
	})
	if err != nil {
		return devices, errors.Wrapf(err, "getting list of devices")
	}
	// back-fill LastSeen status from its bucket
	err = db.lastSeenDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(deviceLastSeenBucket))
		return b.ForEach(func(k, v []byte) error {
			for i := range devices {
				udid := []byte(devices[i].UDID)
				if bytes.Compare(k, udid) == 0 {
					devices[i].LastSeen = decodeTime(b.Get(udid))
					break
				}
			}
			return nil
		})
	})
	return devices, err
}

// decodeTime unmarshals a time.
func decodeTime(b []byte) time.Time {
	i := int64(binary.BigEndian.Uint64(b))
	return time.Unix(i, 0)
}

// encodeTime marshals a time.
func encodeTime(t time.Time) []byte {
	buf := make([]byte, 8)
	u := uint64(t.Unix())
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

// UpdateLastSeen writes the current time for a UDID into the last seen bucket
func (db *DB) UpdateLastSeen(ctx context.Context, udid string) error {
	err := db.lastSeenDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(deviceLastSeenBucket))
		err := b.Put([]byte(udid), encodeTime(time.Now()))
		return err
	})
	return errors.Wrapf(err, "save LastSeen for UDID %s", udid)
}

func (db *DB) Save(ctx context.Context, dev *device.Device) error {
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(DeviceBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", DeviceBucket)
	}
	devproto, err := device.MarshalDevice(dev)
	if err != nil {
		return errors.Wrap(err, "marshalling device")
	}

	// store an array of indices to reference the UUID, which will be the
	// key used to store the actual device.
	indexes := []string{dev.UDID, dev.SerialNumber}
	idxBucket := tx.Bucket([]byte(deviceIndexBucket))
	if idxBucket == nil {
		return fmt.Errorf("bucket %q not found!", deviceIndexBucket)
	}
	for _, idx := range indexes {
		if idx == "" {
			continue
		}
		key := []byte(idx)
		if err := idxBucket.Put(key, []byte(dev.UUID)); err != nil {
			return errors.Wrap(err, "put device to boltdb")
		}
	}

	key := []byte(dev.UUID)
	if err := bkt.Put(key, devproto); err != nil {
		return errors.Wrap(err, "put device to boltdb")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit device to boltdb")
	}
	return db.UpdateLastSeen(ctx, dev.UDID)
}

func (db *DB) DeleteByUDID(ctx context.Context, udid string) error {
	return db.deleteByIndex(udid)
}

func (db *DB) DeleteBySerial(ctx context.Context, serial string) error {
	return db.deleteByIndex(serial)
}

func (db *DB) deleteByIndex(key string) error {
	device, err := db.deviceByIndex(key)
	if err != nil {
		return err
	}

	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}

	bkt := tx.Bucket([]byte(DeviceBucket))
	if err := bkt.Delete([]byte(device.UUID)); err != nil {
		return errors.Wrapf(err, "delete device for key %s", key)
	}

	idxBucket := tx.Bucket([]byte(deviceIndexBucket))
	if err := idxBucket.Delete([]byte(device.UDID)); err != nil {
		return errors.Wrapf(err, "delete device index for UDID %s", device.UDID)
	}
	if err := idxBucket.Delete([]byte(device.SerialNumber)); err != nil {
		return errors.Wrapf(err, "delete device index for serial %s", device.SerialNumber)
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrapf(err, "commiting device deleteion")
	}

	err = db.lastSeenDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(deviceLastSeenBucket))
		err := b.Delete([]byte(device.UDID))
		return err
	})
	return errors.Wrapf(err, "delete LastSeen for UDID %s", device.UDID)
}

type notFound struct {
	ResourceType string
	Message      string
}

func (e *notFound) Error() string {
	return fmt.Sprintf("not found: %s %s", e.ResourceType, e.Message)
}

func (e *notFound) NotFound() bool {
	return true
}

func (db *DB) DeviceByUDID(ctx context.Context, udid string) (*device.Device, error) {
	return db.deviceByIndex(udid)
}

func (db *DB) DeviceBySerial(ctx context.Context, serial string) (*device.Device, error) {
	return db.deviceByIndex(serial)
}

func (db *DB) deviceByIndex(key string) (*device.Device, error) {
	var dev device.Device
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DeviceBucket))
		ib := tx.Bucket([]byte(deviceIndexBucket))
		idx := ib.Get([]byte(key))
		if idx == nil {
			return &notFound{"Device", fmt.Sprintf("key %s", key)}
		}
		v := b.Get(idx)
		if idx == nil {
			return &notFound{"Device", fmt.Sprintf("uuid %s", string(idx))}
		}
		return device.UnmarshalDevice(v, &dev)
	})
	if err != nil {
		return nil, err
	}
	// lookup LastSeen status from its bucket
	err = db.lastSeenDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(deviceLastSeenBucket))
		v := b.Get([]byte(dev.UDID))
		if v != nil {
			dev.LastSeen = decodeTime(v)
		}
		return nil
	})
	return &dev, err
}

func (db *DB) SaveUDIDCertHash(udid, certHash []byte) error {
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(udidCertAuthBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", udidCertAuthBucket)
	}
	if err := bkt.Put(udid, certHash); err != nil {
		return errors.Wrap(err, "put udid cert to boltdb")
	}
	return tx.Commit()
}

func (db *DB) GetUDIDCertHash(udid []byte) ([]byte, error) {
	var certHash []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(udidCertAuthBucket))
		if b == nil {
			return fmt.Errorf("bucket %q not found!", udidCertAuthBucket)
		}
		certHash = b.Get(udid)
		if certHash == nil {
			return &notFound{"UDID", fmt.Sprintf("udid %s", string(udid))}
		}
		return nil
	})
	return certHash, err
}
